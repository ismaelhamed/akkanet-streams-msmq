// Copyright (c) 2023 Ismael Hamed. All rights reserved.
// See LICENSE file in the root folder for full license information.

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
            EnsureQueueIsRecreated(Fixture.SourceQueuePath);
            EnsureQueueIsRecreated(Fixture.DestinationQueuePath);

            var expectedMessagesBodies = new[] { "test-1", "test-2" };

            foreach (var msg in expectedMessagesBodies)
                Queue.Send(new Message(msg), MessageQueueTransactionType.Single);

            var done = MsmqSource.WithTransactionContext(MessageQueueSettings.Default, Fixture.SourceQueuePath)
                .Via(MsmqFlow.WithTransactionContext(MessageQueueSettings.Default, Fixture.DestinationQueuePath))
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
            DLQueuePath.GetAllMessages().Length.Should().Be(expectedMessagesBodies.Length);
        }
    }
}
