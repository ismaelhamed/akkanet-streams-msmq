using System;
using System.Messaging;
using System.Threading.Tasks;
using Akka.Streams.Dsl;
using Akka.Streams.Supervision;
using Akka.Streams.TestKit;
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
        public async Task A_MsmqSource_should_push_available_messages()
        {
            // Send messages to the queue
            await Queue.SendAsync(new Message("Test1"));
            await Queue.SendAsync(new Message("Test2"));
            await Queue.SendAsync(new Message("Test3"));

            var probe = MsmqSource.Create(Queue)
                .Take(3)
                .Select(x => x.Value)
                .RunWith(this.SinkProbe<Message>(), Materializer);

            probe.Request(3);
            probe.ExpectNext<Message>(msg => (string) msg.Body == "Test1");
            probe.ExpectNext<Message>(msg => (string) msg.Body == "Test2");
            probe.ExpectNext<Message>(msg => (string) msg.Body == "Test3");
            probe.ExpectComplete();
        }

        [Fact]
        public async Task A_MsmqSource_should_poll_for_messages_if_the_queue_is_empty()
        {
            // Send a message to the queue
            await Queue.SendAsync(new Message("Test1"));

            var probe = MsmqSource.Create(Queue)
                .Take(3)
                .Select(x => x.Value)
                .RunWith(this.SinkProbe<Message>(), Materializer);

            probe.Request(2).ExpectNext<Message>(msg => (string) msg.Body == "Test1");
            probe.ExpectNoMsg(TimeSpan.FromSeconds(1));

            await Queue.SendAsync(new Message("Test2"));
            await Queue.SendAsync(new Message("Test3"));

            probe.ExpectNext<Message>(msg => (string) msg.Body == "Test2");
            probe.Request(1).ExpectNext<Message>(msg => (string) msg.Body == "Test3");
            probe.Cancel();
        }

        // [Fact]
        // public async Task A_QueueSource_should_only_poll_if_demand_is_available()
        // {
        //     await Queue.SendAsync(new Message("Test1"));
        //
        //     var probe = MsmqSource.Create(Queue)
        //         .Select(x =>
        //         {
        //             Queue.Purge();
        //             return x.Value;
        //         })
        //         .RunWith(this.SinkProbe<Message>(), Materializer);
        //
        //     probe.Request(1).ExpectNext<Message>(msg => (string) msg.Body == "Test1");
        //     await Queue.SendAsync(new Message("Test2"));
        //     probe.ExpectNoMsg(TimeSpan.FromSeconds(3));
        //
        //     // Message wouldn't be visible if the source has called GetMessages even if the message wasn't pushed to the stream
        //     (await Queue.PeekMessagesAsync(1)).Value[0].MessageText.Should().Be("Test2");
        //
        //     probe.Request(1).ExpectNext<Message>(msg => (string) msg.Body == "Test2");
        // }

        [Fact]
        public void A_QueueSource_should_fail_when_an_error_occurs()
        {
            EnsureQueueIsDeleted(_fixture.QueuePath);

            var probe = MsmqSource.Create(Queue)
                .Take(3)
                .Select(x => x.Value)
                .RunWith(this.SinkProbe<Message>(), Materializer);

            Output.WriteLine(probe.Request(1).ExpectError().Message);
        }

        [Fact]
        public async Task A_QueueSource_should_not_fail_if_the_supervision_strategy_is_not_stop_when_an_error_occurs()
        {
            EnsureQueueIsDeleted(_fixture.QueuePath);

            var probe = MsmqSource.Create(Queue)
                .Take(3)
                .Select(x => x.Value)
                .WithAttributes(ActorAttributes.CreateSupervisionStrategy(Deciders.ResumingDecider))
                .RunWith(this.SinkProbe<Message>(), Materializer);

            probe.Request(3).ExpectNoMsg();

            EnsureQueueExists(_fixture.QueuePath);

            await Queue.SendAsync(new Message("Test1"));
            await Queue.SendAsync(new Message("Test2"));
            await Queue.SendAsync(new Message("Test3"));

            probe.ExpectNext<Message>(msg => (string) msg.Body == "Test1");
            probe.ExpectNext<Message>(msg => (string) msg.Body == "Test2");
            probe.ExpectNext<Message>(msg => (string) msg.Body == "Test3");
            probe.ExpectComplete();
        }
    }
}