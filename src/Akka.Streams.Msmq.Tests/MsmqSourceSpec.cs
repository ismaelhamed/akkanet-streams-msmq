using System;
using System.Messaging;
using System.Threading;
using System.Threading.Tasks;
using Akka.Streams.Dsl;
using Akka.Streams.TestKit;
using NSubstitute;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Streams.Msmq.Tests
{
    public class MsmqSourceSpec : MsmqSpecBase, IClassFixture<MessageQueueFixture>
    {
        private readonly MessageQueueFixture fixture;
        private readonly ITestOutputHelper output;
        private readonly IMessageQueue queue;

        public MsmqSourceSpec(MessageQueueFixture fixture, ITestOutputHelper output)
        {
            this.fixture = fixture;
            this.output = output;
            this.queue = Substitute.For<IMessageQueue>();
        }

        [Fact]
        public void MsmqSource_should_return_events()
        {
            var probe = this.CreateManualSubscriberProbe<Message>();

            queue.ReceiveAsync()
                .Returns(
                    Task.FromResult(new Message("0")),
                    Task.FromResult(new Message("1")),
                    Task.FromResult(new Message("2")));

            MsmqSource.Create(queue)
                .To(Sink.FromSubscriber(probe))
                //.To(Sink.ForEach<Message>(m => output.WriteLine("Body [{0}]", (string)m.Body)))
                .Run(Materializer);

            // // Simulate doing other work on the current thread.
            // Thread.Sleep(TimeSpan.FromSeconds(3));

            var sub = probe.ExpectSubscription();
            sub.Request(3);
            probe.ExpectNext((Message msg) => (string) msg.Body == "0");
            probe.ExpectNext((Message msg) => (string) msg.Body == "1");

            sub.Cancel();
        }

        [Fact]
        public void MsmqSource_should_continue_after_returning_empty_result()
        {
            var probe = this.CreateManualSubscriberProbe<Message>();

            queue.ReceiveAsync()
                .Returns(
                    x => Task.FromResult(new Message("0")),
                    x => Task.FromResult(new Message("1")),
                    x => Task.FromResult(new Message("2")),
                    //x => { throw new Exception(); },
                    x => null,
                    x => Task.FromResult(new Message("4")));

            MsmqSource.Create(queue)
                .To(Sink.FromSubscriber(probe))
                .Run(Materializer);

            var sub = probe.ExpectSubscription();
            sub.Request(3);
            probe.ExpectNext((Message msg) => (string) msg.Body == "0");
            probe.ExpectNext((Message msg) => (string) msg.Body == "1");
            probe.ExpectNext((Message msg) => (string) msg.Body == "2");
            sub.Request(2);
            probe.ExpectNext((Message msg) => (string) msg.Body == "3");
            sub.Cancel();
        }
    }
}