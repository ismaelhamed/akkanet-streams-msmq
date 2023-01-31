// Copyright (c) 2023 Ismael Hamed. All rights reserved.
// See LICENSE file in the root folder for full license information.

using System.Messaging;
using System.Threading.Tasks;
using Akka.Streams.Dsl;

namespace Akka.Streams.Msmq
{
    public static class MsmqSink
    {
        /// <summary>
        /// Creates a <see cref="Sink"/> that sends to MSMQ for every stream element.
        /// <para>
        /// Depending on whether the queue is transactional or not, this method could behave differently:
        /// <list type="bullet">
        ///   <item>
        ///     <term>Transactional</term>
        ///     <description>Sends each message as a single internal transaction.</description>
        ///   </item>
        ///   <item>
        ///     <term>Non-transactional</term>
        ///     <description>Sends each message to a non-transactional thread.</description>
        ///   </item>
        /// </list>
        /// </para>
        /// </summary>
        /// <param name="settings">Settings to configure the <see cref="MessageQueue"/>.</param>
        /// <param name="queuePath">The location of the queue referenced by the underlying <see cref="MessageQueue"/>.
        /// The syntax for <paramref name="queuePath"/> depends on the type of queue it points to, as shown in the following
        /// <see href="https://learn.microsoft.com/en-us/dotnet/api/system.messaging.messagequeue.path">table</see>.
        /// </param>
        public static Sink<Message, Task> Default(MessageQueueSettings settings, string queuePath) =>
            MsmqFlow.Default(settings, queuePath)
                .ToMaterialized(Sink.Ignore<Message>(), Keep.Right);

        /// <summary>
        /// Creates a <see cref="Sink"/> that sends to MSMQ for every stream element and is aware of the
        /// <see cref="MessageQueueTransaction"/> passed as context from a <see cref="MsmqFlow.WithTransactionContext"/>.
        /// </summary>
        /// <param name="settings">Settings to configure the <see cref="MessageQueue"/>.</param>
        /// <param name="queuePath">The location of the queue referenced by the underlying <see cref="MessageQueue"/>.
        /// The syntax for <paramref name="queuePath"/> depends on the type of queue it points to, as shown in the following
        /// <see href="https://learn.microsoft.com/en-us/dotnet/api/system.messaging.messagequeue.path">table</see>.
        /// </param>
        public static Sink<(Message, MessageQueueTransaction), Task> WithTransactionContext(MessageQueueSettings settings, string queuePath) =>
            Flow.Create<(Message, MessageQueueTransaction)>()
                .Via(MsmqFlow.WithTransactionContext(settings, queuePath))
                .SelectAsyncUnordered(settings.Parallelism, tuple =>
                {
                    // then commit transaction
                    var (message, trx) = tuple;
                    trx.Commit();
                    trx.Dispose();

                    return Task.FromResult(message);
                })
                .ToMaterialized(Sink.Ignore<Message>(), Keep.Right);
    }
}