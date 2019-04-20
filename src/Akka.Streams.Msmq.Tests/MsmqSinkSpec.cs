using System;
using System.Linq;
using System.Messaging;
using Akka.Streams.Dsl;
using Akka.Streams.TestKit;
using FluentAssertions;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Streams.Msmq.Tests
{
    // https://docs.particular.net/transports/msmq/operations-scripting
    // https://csharp.hotexamples.com/examples/System.Messaging/MessageQueueTransaction/Commit/php-messagequeuetransaction-commit-method-examples.html
    // https://github.com/pmacn/Messaging/blob/master/src/Messaging/MessageQueueExtensions.cs

    public class MsmqSinkSpec : MsmqSpecBase, IClassFixture<MessageQueueFixture>
    {
        private readonly MessageQueueFixture fixture;
        private readonly ITestOutputHelper output;

        public MsmqSinkSpec(MessageQueueFixture fixture, ITestOutputHelper output)
        {
            this.fixture = fixture;
            this.output = output;
        }

        [Fact]
        public void A_MsmqSink_Should_Add_Elements_To_The_Queue()
        {
            var messages = Enumerable.Range(0, 5)
                .Select(i => new SomeMessage() { SomeProperty = i });

            var task = Source.From(messages)
                .Select(m => new Message(m))
                .RunWith(MsmqSink.Create(fixture.Queue), Materializer);

            task.Wait(TimeSpan.FromSeconds(3)).Should().BeTrue();
            fixture.Queue.GetAllMessages().Length.Should().Be(5);
        }

        [Fact]
        public void A_MsmqSink_Should_Set_The_Exception_Of_The_Task_When_An_Error_Occurs()
        {
            var (probe, task) = this.SourceProbe<string>()
                .Select(x => new Message(x))
                .ToMaterialized(MsmqSink.Create(fixture.Queue), Keep.Both)
                .Run(Materializer);

            probe.SendError(new Exception("Boom"));
            task.Invoking(x => x.Wait(TimeSpan.FromSeconds(3))).Should().Throw<Exception>().WithMessage("Boom");
        }

        //[Fact]
        //public void A_QueueSink_should_retry_failing_messages_if_supervision_strategy_is_resume()
        //{
        //    EnsureQueueIsDeleted(fixture.QueuePath);

        //    var messages = new[] { "{\"Value\":\"1\"}", "{\"Value\":\"2\"}" };
        //    var queueSink = MsmqSink.Create(fixture.Queue)
        //        .WithAttributes(ActorAttributes.CreateSupervisionStrategy(Deciders.ResumingDecider));

        //    var task = Source.From(messages)
        //        .Select(x => new Message(x))
        //        .RunWith(queueSink, Materializer);

        //    Thread.Sleep(5);
        //    EnsureQueueExists(fixture.QueuePath);
            
        //    task.Wait(TimeSpan.FromSeconds(3)).Should().BeTrue();
        //    fixture.Queue.GetAllMessages().Length.Should().Be(2);
        //}

        //[Fact]
        //public void A_QueueSink_should_skip_failing_messages_if_supervision_strategy_is_restart()
        //{
        //    EnsureQueueIsDeleted(fixture.QueuePath);

        //    var queueSink = MsmqSink.Create(fixture.Queue)
        //        .WithAttributes(ActorAttributes.CreateSupervisionStrategy(Deciders.RestartingDecider));

        //    var (probe, task) = this.SourceProbe<string>()
        //        .Select(x => new Message(x))
        //        .ToMaterialized(queueSink, Keep.Both)
        //        .Run(Materializer);
            
        //    probe.SendNext("1");
        //    Thread.Sleep(5);
        //    EnsureQueueExists(fixture.QueuePath);

        //    probe.SendNext("2");
        //    probe.SendComplete();
        //    task.Wait(TimeSpan.FromSeconds(3)).Should().BeTrue();
        //    fixture.Queue.GetAllMessages().Length.Should().Be(2);
        //}
    }
}
