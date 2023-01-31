using System;
using System.Linq;
using System.Messaging;
using System.Threading;
using Akka.Streams.Dsl;
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
            var queue = new MessageQueue(Fixture.QueuePath);
            queue.Send(new Message("alpakka"), MessageQueueTransactionType.Single);

            var future = MsmqSource.Default(MessageQueueSettings.Default, Fixture.QueuePath)
                .RunWith(Sink.First<Message>(), Materializer);

            future.Result.Body.Should().Be("alpakka");
        }

        [IgnoreOnGitHubFact]
        public void MsmqSource_must_continue_streaming_if_receives_an_empty_response()
        {
            var (killSwitch, future) = MsmqSource.Default(MessageQueueSettings.Default, Fixture.QueuePath)
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
            var future = MsmqSource.Default(MessageQueueSettings.Default, $"{Fixture.QueuePath}/not-existing")
                .RunWith(Sink.Seq<Message>(), Materializer);

            var exception = Assert.Throws<MessageQueueException>(() => AwaitResult(future));
            Assert.StartsWith("The queue does not exist", exception.Message);
        }

        [IgnoreOnGitHubFact]
        public void MsmqSource_with_ack_should_be_able_to_commit()
        {
            const int numberOfMessages = 100;
            const string queuePath = @".\Private$\MsmqSpecQueueWithTrx";

            var queue = new MessageQueue(queuePath, QueueAccessMode.Send);
            Enumerable.Range(0, numberOfMessages).ForEach(i => queue.Send(new Message($"alpakka-{i}"), MessageQueueTransactionType.Single));

            var latch = new CountdownEvent(numberOfMessages);
            var (killSwitch, future) = MsmqSource.Committable(MessageQueueSettings.Default, queuePath)
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
            const string queuePath = @".\Private$\MsmqSpecQueueWithTrx";
            var queue = new MessageQueue(queuePath, QueueAccessMode.Send);
            queue.Send(new Message("alpakka"), MessageQueueTransactionType.Single);

            var future = MsmqSource.Committable(MessageQueueSettings.Default, queuePath)
              .Take(1)
              .RunWith(Sink.Seq<ICommittableMessage>(), Materializer);

            future.Result.Count.Should().Be(1);

            var future2 = MsmqSource.Committable(MessageQueueSettings.Default, queuePath)
                .TakeWithin(TimeSpan.FromMilliseconds(200))
                .RunWith(Sink.Seq<ICommittableMessage>(), Materializer);

            // trx is still pending, so message is not visible for others
            future2.Result.Count.Should().Be(0);

            // now abort trx
            future.Result.First().Abort();

            var future3 = MsmqSource.Committable(MessageQueueSettings.Default, queuePath)
              .TakeWithin(TimeSpan.FromSeconds(50))
              .RunWith(Sink.Seq<ICommittableMessage>(), Materializer);

            // if we wait enough time, message will be returned to the queue
            future3.Result.Count.Should().Be(1);
            future3.Result.First().Commit();
        }

        [IgnoreOnGitHubFact]
        public void MsmqSource_stream_from_transactional_should_be_able_to_access_trx_when_used_with_flowWithContext()
        {
            const int numberOfMessages = 100;
            const string queuePath = @".\Private$\MsmqSpecQueueWithTrx";

            var queue = new MessageQueue(queuePath, QueueAccessMode.Send);
            Enumerable.Range(0, numberOfMessages).ForEach(i => queue.Send(new Message($"alpakka-{i}"), MessageQueueTransactionType.Single));

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

            var (killSwitch, future) = MsmqSource.WithTransactionContext(MessageQueueSettings.Default, queuePath)
                .Via(flowWithContext)
                .ViaMaterialized(KillSwitches.Single<(Message, Done)>(), Keep.Right)
                .ToMaterialized(Sink.Seq<(Message, Done)>(), Keep.Both)
                .Run(Materializer);

            latch.Wait();
            killSwitch.Shutdown();

            future.Result.Count.Should().Be(numberOfMessages);
        }
    }
}
