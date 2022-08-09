using System;
using System.Linq;
using System.Messaging;
using System.Threading;
using Akka.Streams.Dsl;
using Akka.Streams.Supervision;
using Akka.Streams.TestKit;
using FluentAssertions;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Streams.Msmq.Tests
{
    [Collection("MsmqQueueSpec")]
    public class MsmqSinkSpec : MsmqSpecBase, IClassFixture<MessageQueueFixture>
    {
        private readonly MessageQueueFixture _fixture;

        public MsmqSinkSpec(MessageQueueFixture fixture, ITestOutputHelper output)
            : base(fixture, output) => _fixture = fixture;

        [Fact]
        public void MsmqSink_should_add_elements_to_the_queue()
        {
            EnsureQueueIsRecreated(_fixture.QueuePath);

            var messages = Enumerable.Range(0, 5)
                .Select(i => new Message(i));

            var msmqSink = MsmqSink.Default(MessageQueueSettings.Default, _fixture.QueuePath);

            var future = Source.From(messages)
                .RunWith(msmqSink, Materializer);

            future.Wait(TimeSpan.FromSeconds(3)).Should().BeTrue();
            Queue.GetAllMessages().Length.Should().Be(5);
        }

        [Fact]
        public void MsmqSink_should_retry_failing_messages_if_supervision_strategy_is_resume()
        {
            EnsureQueueIsDeleted(_fixture.QueuePath);

            var messages = new[] { "{\"Value\":\"1\"}", "{\"Value\":\"2\"}" };
            var queueSink = MsmqSink.Default(MessageQueueSettings.Default, _fixture.QueuePath)
                .WithAttributes(ActorAttributes.CreateSupervisionStrategy(Deciders.ResumingDecider));

            var future = Source.From(messages)
                .Select(x => new Message(x))
                .RunWith(queueSink, Materializer);

            Thread.Sleep(5);
            EnsureQueueExists(_fixture.QueuePath);

            future.Wait(TimeSpan.FromSeconds(3)).Should().BeTrue();
            Queue.GetAllMessages().Length.Should().Be(2);
        }

        [Fact]
        public void MsmqSink_should_skip_failing_messages_if_supervision_strategy_is_restart()
        {
            EnsureQueueIsDeleted(_fixture.QueuePath);

            var queueSink = MsmqSink.Default(MessageQueueSettings.Default, _fixture.QueuePath)
                .WithAttributes(ActorAttributes.CreateSupervisionStrategy(Deciders.RestartingDecider));

            var (probe, future) = this.SourceProbe<string>()
                .Select(x => new Message(x))
                .ToMaterialized(queueSink, Keep.Both)
                .Run(Materializer);

            probe.SendNext("1");            
            EnsureQueueExists(_fixture.QueuePath);
            Thread.Sleep(5);

            probe.SendNext("2");
            probe.SendComplete();
            future.Wait(TimeSpan.FromSeconds(3)).Should().BeTrue();
            Queue.GetAllMessages().Length.Should().Be(2);
        }
    }
}