// Copyright (c) 2023 Ismael Hamed. All rights reserved.
// See LICENSE file in the root folder for full license information.

using System;
using System.Messaging;
using System.Threading;
using System.Threading.Tasks;
using Akka.Actor;
using Xunit;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace Akka.Streams.Msmq.Tests
{
    [CollectionDefinition("MsmqQueueSpec", DisableParallelization = true)]
    public abstract class MsmqSpecBase : Akka.TestKit.Xunit2.TestKit, IClassFixture<MessageQueueFixture>
    {
        protected readonly MessageQueueFixture Fixture;

        protected ActorMaterializer Materializer { get; }

        protected MessageQueue Queue { get; }

        protected MsmqSpecBase(MessageQueueFixture fixture, ITestOutputHelper output)
            : base((ActorSystem)null, output)
        {
            Fixture = fixture;
            Materializer = Sys.Materializer();
            Queue = new MessageQueue(@".\Private$\MsmqSpecQueue")
            {
                Formatter = fixture.Formatter
            };
        }

        protected static void EnsureQueueExists(string queuePath, bool transactional = true)
        {
            if (!MessageQueue.Exists(queuePath))
                MessageQueue.Create(queuePath, transactional);
        }

        protected static void EnsureQueueIsDeleted(string queuePath)
        {
            if (MessageQueue.Exists(queuePath))
                MessageQueue.Delete(queuePath);
        }

        protected static void EnsureQueueIsRecreated(string queuePath, bool transactional = true)
        {
            if (MessageQueue.Exists(queuePath))
                MessageQueue.Delete(queuePath);

            _ = MessageQueue.Create(queuePath, transactional);
        }

        public new void Dispose() => Queue.Purge();

        public static T AwaitResult<T>(Task<T> assertionTask, TimeSpan? atMost = null)
        {
            try
            {
                return Awaitable(assertionTask, atMost).Result;
            }
            catch (Exception ex) when (!(ex is TimeoutException))
            {
                throw ex is AggregateException aggregateException
                    ? aggregateException.Flatten().InnerExceptions[0]
                    : ex;
            }
        }

        private static async Task<T> Awaitable<T>(Task<T> assertionTask, TimeSpan? atMost = null)
        {
            var cts = new CancellationTokenSource();
            try
            {
                var delayTask = Task.Delay(atMost ?? TimeSpan.FromSeconds(5), cts.Token);
                var completedTask = await Task.WhenAny(assertionTask, delayTask);
                return completedTask == delayTask ? throw new TimeoutException() : await assertionTask;
            }
            finally
            {
                cts.Cancel();
                cts.Dispose();
            }
        }
    }
}
