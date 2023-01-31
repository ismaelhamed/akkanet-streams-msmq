using System;
using System.Messaging;
using Akka.Streams.Dsl;
using FluentAssertions;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Streams.Msmq.Tests
{
    [Collection("MsmqQueueSpec")]    
    public class MsmqFlowSpec : MsmqSpecBase
    {
        public MsmqFlowSpec(MessageQueueFixture fixture, ITestOutputHelper output)
            : base(fixture, output)
        { }

        [IgnoreOnGitHubFact]
        public void MsmqFlow_must_receive_from_queue_and_send_in_the_same_transaction()
        {
            const string receivingQueue = @".\Private$\ReceivingQueue";
            const string sendingQueue = @".\Private$\SendingQueue";

            EnsureQueueIsRecreated(receivingQueue, transactional: true);
            EnsureQueueIsRecreated(sendingQueue, transactional: true);

            var expectedMessagesBodies = new[] { "test-1", "test-2" };

            var queue = new MessageQueue(receivingQueue, QueueAccessMode.SendAndReceive);
            foreach (var msg in expectedMessagesBodies)
                queue.Send(new Message(msg), MessageQueueTransactionType.Single);

            var done = MsmqSource.WithTransactionContext(MessageQueueSettings.Default, receivingQueue)
                .Via(MsmqFlow.WithTransactionContext(MessageQueueSettings.Default, sendingQueue))
                .AsSource()
                .TakeWithin(TimeSpan.FromMilliseconds(200))
                .Select(tuple =>
                {
                    // then commit transaction
                    var (msg, trx) = tuple;
                    trx.Commit();
                    trx.Dispose();

                    return msg;
                })
                .ToMaterialized(Sink.Seq<Message>(), Keep.Right)
                .Run(Sys.Materializer());

            done.Result.Count.Should().Be(expectedMessagesBodies.Length);
        }
    }
}
