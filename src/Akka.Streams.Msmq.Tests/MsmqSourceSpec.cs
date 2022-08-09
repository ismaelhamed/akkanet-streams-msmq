using System;
using System.Diagnostics;
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
    public class MsmqSourceSpec : MsmqSpecBase, IClassFixture<MessageQueueFixture>
    {
        private readonly MessageQueueFixture _fixture;

        public MsmqSourceSpec(MessageQueueFixture fixture, ITestOutputHelper output)
            : base(fixture, output) => _fixture = fixture;

        [Fact]
        public void MsmqSource_must_stream_a_single_message_from_the_queue()
        {
            var queue = new MessageQueue(_fixture.QueuePath);
            queue.Send(new Message("alpakka"));

            var future = MsmqSource.Create(_fixture.QueuePath, MessageQueueSettings.Default)
                .RunWith(Sink.First<Message>(), Materializer);

            future.Result.Body.Should().Be("alpakka");
        }

        [Fact]
        public void MsmqSource_must_continue_streaming_if_receives_an_empty_response()
        {
            var (killSwitch, future) = MsmqSource.Create(_fixture.QueuePath, MessageQueueSettings.Default)
                .ViaMaterialized(KillSwitches.Single<Message>(), Keep.Right)
                .ToMaterialized(Sink.Ignore<Message>(), Keep.Both)
                .Run(Materializer);

            // make sure the source polled sqs once for an empty response
            Thread.Sleep(TimeSpan.FromSeconds(1));

            future.IsCompleted.Should().BeFalse();
            killSwitch.Shutdown();
        }

        [Fact]
        public void MsmqSource_must_finish_immediately_if_the_queue_does_not_exist()
        {
            var future = MsmqSource.Create($"{_fixture.QueuePath}/not-existing", MessageQueueSettings.Default)
                .RunWith(Sink.Seq<Message>(), Materializer);

            var exception = Assert.Throws<MessageQueueException>(() => AwaitResult(future));
            Assert.StartsWith("The queue does not exist", exception.Message);
        }

        [Fact]
        public void MsmqSource_stream_single_message_at_least_twice_from_the_queue_when_visibility_timeout_passed()
        {
            const string queuePath = @".\Private$\MsmqSpecQueueWithTrx";
            var queue = new MessageQueue(queuePath, QueueAccessMode.Send);
            queue.Send(new Message("alpakka"));

            var future = MsmqSource.CreateWithAck(queuePath, MessageQueueSettings.Default)
              .Take(1)
              .RunWith(Sink.Seq<ICommittableMessage>(), Materializer);

            future.Result.Count.Should().Be(1);

            var future2 = MsmqSource.CreateWithAck(queuePath, MessageQueueSettings.Default)
              .TakeWithin(TimeSpan.FromMilliseconds(200))
              .RunWith(Sink.Seq<ICommittableMessage>(), Materializer);

            future2.Result.Count.Should().Be(0);
            future.Result.First().Abort();

            var future3 = MsmqSource.CreateWithAck(queuePath, MessageQueueSettings.Default)
              .TakeWithin(TimeSpan.FromSeconds(30))
              .RunWith(Sink.Seq<ICommittableMessage>(), Materializer);

            future3.Result.Count.Should().Be(1);
            future3.Result.First().Commit();
        }

        [Fact]
        public void MsmqSource_stream_single_message_at_least_twice_from_the_queue_when_visibility_timeout_passed1()
        {
            const int numberOfMessages = 10_000;
            const string queuePath = @".\Private$\MsmqSpecQueueWithTrx";

            var queue = new MessageQueue(queuePath, QueueAccessMode.Send);
            Enumerable.Range(0, numberOfMessages).ForEach(i => queue.Send(new Message($"alpakka-{i}"), MessageQueueTransactionType.Single));

            var sw = Stopwatch.StartNew();

            var counter = 0;
            var flowWithContext = FlowWithContext.Create<Message, MessageQueueTransaction>()
                .SelectContext(trx =>
                {
                    // then commit transaction
                    trx.Commit();
                    trx.Dispose();

                    return Done.Instance;
                })
                .AsFlow()
                .WireTap(tuple =>
                {
                    counter++;
                    if (counter < numberOfMessages) return;

                    sw.Stop();
                    Output.WriteLine("[{0}] {1}", DateTime.UtcNow.ToString(), tuple.Item1.Body.ToString());
                });

            Output.WriteLine("[{0}] receiving...", DateTime.Now);
            var (killSwitch, future) = MsmqSource.CreateWithContext(queuePath, MessageQueueSettings.Default)
                .Via(flowWithContext)
                .ViaMaterialized(KillSwitches.Single<(Message, Done)>(), Keep.Right)            
                .ToMaterialized(Sink.Seq<(Message, Done)>(), Keep.Both)
                .Run(Materializer);

            Thread.Sleep(3000);
            killSwitch.Shutdown();
            Output.WriteLine("[{0}] shutting down...", DateTime.Now);

            future.Result.Count.Should().Be(numberOfMessages);
            Output.WriteLine("Test took {0} secs.", sw.Elapsed.TotalSeconds);
        }

        [Fact]
        public void MsmqSource_stream_single_message_at_least_twice_from_the_queue_when_visibility_timeout_passed2()
        {
            const int numberOfMessages = 10_000;
            const string queuePath = @".\Private$\MsmqSpecQueueWithTrx";

            var queue = new MessageQueue(queuePath, QueueAccessMode.Send);
            Enumerable.Range(0, numberOfMessages).ForEach(i => queue.Send(new Message($"alpakka-{i}"), MessageQueueTransactionType.Single));

            var sw = Stopwatch.StartNew();

            //var counter = 0;
            //Output.WriteLine("[{0}] receiving...", DateTime.Now);
            //var (killSwitch, future) = MsmqSource.Create(queuePath, MessageQueueSettings.Default)
            //    .ViaMaterialized(KillSwitches.Single<Message>(), Keep.Right)
            //    .WireTap(msg =>
            //    {
            //        counter++;
            //        if (counter < numberOfMessages) return;

            //        sw.Stop();
            //        Output.WriteLine("[{0}] {1}", DateTime.UtcNow.ToString(), msg.Body.ToString());
            //    })
            //    .ToMaterialized(Sink.Seq<Message>(), Keep.Both)
            //    .Run(Materializer);

            var counter = 0;
            Output.WriteLine("[{0}] receiving...", DateTime.Now);
            var (killSwitch, future) = MsmqSource.CreateWithAck(queuePath, MessageQueueSettings.Default)
                .ViaMaterialized(KillSwitches.Single<ICommittableMessage>(), Keep.Right)
                .Select(msg =>
                {
                    // then commit transaction
                    msg.Commit();
                    return msg.Message;
                })
                .WireTap(msg =>
                {
                    counter++;
                    if (counter < numberOfMessages) return;

                    sw.Stop();
                    Output.WriteLine("[{0}] {1}", DateTime.UtcNow.ToString(), msg.Body.ToString());
                })
                .ToMaterialized(Sink.Seq<Message>(), Keep.Both)
                .Run(Materializer);

            Thread.Sleep(3000);
            killSwitch.Shutdown();
            Output.WriteLine("[{0}] shutting down...", DateTime.Now);

            future.Result.Count.Should().Be(numberOfMessages);
            Output.WriteLine("Test took {0} secs.", sw.Elapsed.TotalSeconds);
        }

        //protected override void AfterAll()
        //{
        //    base.AfterAll();
        //    MessageQueue.Delete(_fixture.QueuePath);
        //}
    }
}
