// Copyright (c) 2023 Ismael Hamed. All rights reserved.
// See LICENSE file in the root folder for full license information.

using System;
using System.IO;
using System.Linq;
using System.Messaging;
using System.Text;
using System.Threading;
using System.Xml;
using Akka.Streams.Dsl;
using Akka.Util;
using Akka.Util.Internal;
using FluentAssertions;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Streams.Msmq.Tests
{
    [Collection("MsmqQueueSpec")]
    public class MsmqSourceSpec : MsmqSpecBase
    {
        public MsmqSourceSpec(MessageQueueFixture fixture, ITestOutputHelper output)
            : base(fixture, output)
        { }

        [IgnoreOnGitHubFact]
        public void MsmqSource_must_stream_a_single_message_from_the_queue()
        {
            EnsureQueueIsRecreated(Fixture.SourceQueuePath);

            Queue.Send(new Message("alpakka"), MessageQueueTransactionType.Single);

            var future = MsmqSource.Default(MessageQueueSettings.Default, Fixture.SourceQueuePath)
                .RunWith(Sink.First<Message>(), Materializer);

            future.Result.Body.Should().Be("alpakka");
        }

        [IgnoreOnGitHubFact]
        public void MsmqSource_must_continue_streaming_if_receives_an_empty_response()
        {
            EnsureQueueIsRecreated(Fixture.SourceQueuePath);

            var (killSwitch, future) = MsmqSource.Default(MessageQueueSettings.Default, Fixture.SourceQueuePath)
                .ViaMaterialized(KillSwitches.Single<Message>(), Keep.Right)
                .ToMaterialized(Sink.Ignore<Message>(), Keep.Both)
                .Run(Materializer);

            // make sure the source polled sqs once for an empty response
            Thread.Sleep(TimeSpan.FromSeconds(1));

            future.IsCompleted.Should().BeFalse();
            killSwitch.Shutdown();
        }

        [IgnoreOnGitHubFact]
        public void MsmqSource_must_finish_immediately_if_the_queue_does_not_exist()
        {
            EnsureQueueIsDeleted(Fixture.SourceQueuePath);

            var future = MsmqSource.Default(MessageQueueSettings.Default, $"{Fixture.SourceQueuePath}/not-existing")
                .RunWith(Sink.Seq<Message>(), Materializer);

            var exception = Assert.Throws<MessageQueueException>(() => AwaitResult(future));
            Assert.StartsWith("The queue does not exist", exception.Message);
        }

        [IgnoreOnGitHubFact]
        public void MsmqSource_with_ack_should_be_able_to_commit()
        {
            EnsureQueueIsRecreated(Fixture.SourceQueuePath);

            const int numberOfMessages = 10;

            Enumerable.Range(0, numberOfMessages).ForEach(i => Queue.Send(new Message($"alpakka-{i}"), MessageQueueTransactionType.Single));

            var latch = new CountdownEvent(numberOfMessages);
            var (killSwitch, future) = MsmqSource.Committable(MessageQueueSettings.Default, Fixture.SourceQueuePath)
                .ViaMaterialized(KillSwitches.Single<ICommittableMessage>(), Keep.Right)
                .Select(committable =>
                {
                    // then commit transaction
                    committable.Commit();
                    return committable.Message;
                })
                .WireTap(_ => latch.Signal())
                .ToMaterialized(Sink.Seq<Message>(), Keep.Both)
                .Run(Materializer);

            latch.Wait();
            killSwitch.Shutdown();

            future.Result.Count.Should().Be(numberOfMessages);
        }

        [IgnoreOnGitHubFact]
        public void MsmqSource_stream_single_message_at_least_twice_from_the_queue_when_trx_is_aborted()
        {
            EnsureQueueIsRecreated(Fixture.SourceQueuePath);

            Queue.Send(new Message("alpakka"), MessageQueueTransactionType.Single);

            var future = MsmqSource.Committable(MessageQueueSettings.Default, Fixture.SourceQueuePath)
              .Take(1)
              .RunWith(Sink.Seq<ICommittableMessage>(), Materializer);

            future.Result.Count.Should().Be(1);

            var future2 = MsmqSource.Committable(MessageQueueSettings.Default, Fixture.SourceQueuePath)
                .TakeWithin(TimeSpan.FromMilliseconds(200))
                .RunWith(Sink.Seq<ICommittableMessage>(), Materializer);

            // trx is still pending, so message is not visible for others
            future2.Result.Count.Should().Be(0);

            // now abort trx
            future.Result[0].Abort();

            var future3 = MsmqSource.Committable(MessageQueueSettings.Default, Fixture.SourceQueuePath)
              .TakeWithin(TimeSpan.FromSeconds(50))
              .RunWith(Sink.Seq<ICommittableMessage>(), Materializer);

            // if we wait enough time, message will be returned to the queue
            future3.Result.Count.Should().Be(1);
            future3.Result[0].Commit();
        }

        [IgnoreOnGitHubFact]
        public void MsmqSource_stream_single_message_at_most_maxRetries_times()
        {
            EnsureQueueIsRecreated(Fixture.SourceQueuePath);
            EnsureQueueIsRecreated(Fixture.DestinationQueuePath);

            const int MaxRetries = 3;

            Queue.Send(new Message() { BodyStream = new MemoryStream(Encoding.UTF8.GetBytes(@"?xml version=""1.0""?>")) }, MessageQueueTransactionType.Single);

            static Either<(Message, Exception), Message> DeserializeMessage(Message message)
            {
                try
                {
                    var msg = message.Body; // Boom!, failed to deserialize
                    return Either.Right(message);
                }
                catch (XmlException ex)
                {
                    return Either.Left((message, (Exception)ex));
                }
            }

            static bool ShouldSendToDlq(Either<(Message, Exception), Message> either)
            {
                if (either.IsRight) return false;

                var (msg, _) = either.ToLeft().Value;

                // Send to DLQ after retries have been exhausted
                return msg.AppSpecific >= MaxRetries;
            }

            var retrySink = Flow.Create<Either<(Message, Exception), Message>>()
                .Collect(either => either.IsLeft, either => either.ToLeft().Value) // Bypass retry logic if not a failure
                .Select(tuple =>
                {
                    var (msg, _) = tuple;
                    msg.Priority = MessagePriority.Lowest; // Lower the priority to be fair with other messages in the queue
                    msg.AppSpecific++; // Use `AppSpecific` field to keep track of the number of retries

                    return msg;
                })
                .To(MsmqSink.Default(MessageQueueSettings.Default, Fixture.SourceQueuePath));

            var deadLetterSink = Flow.Create<Either<(Message, Exception), Message>>()
                .Collect(either => either.IsLeft, either => either.ToLeft().Value)
                .Select(tuple => tuple.Item1)
                .To(MsmqSink.Default(MessageQueueSettings.Default, Fixture.DestinationQueuePath));

            var deserializeFlow = Flow.Create<Message>()
                .Select(DeserializeMessage);

            var deserializeFlowDiverted = deserializeFlow
                .DivertTo(retrySink, e => !ShouldSendToDlq(e))
                .DivertTo(deadLetterSink, ShouldSendToDlq)
                .Collect(either => either.IsRight, either => either.ToRight().Value);

            var future = MsmqSource.Default(MessageQueueSettings.Default, Fixture.SourceQueuePath)
                .TakeWithin(TimeSpan.FromSeconds(5))
                .Via(deserializeFlowDiverted)
                .RunWith(Sink.Seq<Message>(), Materializer);

            future.Result.Count.Should().Be(0);
            DLQueuePath.GetAllMessages().Length.Should().Be(1);
        }

        [IgnoreOnGitHubFact]
        public void MsmqSource_stream_from_transactional_should_be_able_to_access_trx_when_used_with_flowWithContext()
        {
            EnsureQueueExists(Fixture.SourceQueuePath);

            const int numberOfMessages = 10;
            Enumerable.Range(0, numberOfMessages).ForEach(i => Queue.Send(new Message($"alpakka-{i}"), MessageQueueTransactionType.Single));

            var latch = new CountdownEvent(numberOfMessages);
            var flowWithContext = FlowWithContext.Create<Message, MessageQueueTransaction>()
                .SelectContext(trx =>
                {
                    // then commit transaction
                    trx.Commit();
                    trx.Dispose();

                    return Done.Instance;
                })
                .AsFlow()
                .WireTap(tuple => latch.Signal());

            var (killSwitch, future) = MsmqSource.WithTransactionContext(MessageQueueSettings.Default, Fixture.SourceQueuePath)
                .Via(flowWithContext)
                .ViaMaterialized(KillSwitches.Single<(Message, Done)>(), Keep.Right)
                .ToMaterialized(Sink.Seq<(Message, Done)>(), Keep.Both)
                .Run(Materializer);

            latch.Wait();
            killSwitch.Shutdown();

            var result = future.Result;
            result.Count.Should().Be(numberOfMessages);
        }
    }
}
