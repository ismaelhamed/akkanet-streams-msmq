using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Messaging;
using Akka.Streams.Dsl;

namespace Akka.Streams.Msmq
{
    public static class MsmqFlow
    {
        internal static Flow<Message, Done, NotUsed> Default(MessageQueue queue, MessageQueueSettings settings) =>
            Flow.Create<Message>()
                .SelectAsync(settings.Parallelism, async message =>
                {
                    await queue.SendAsync(message).ConfigureAwait(false);
                    return Done.Instance;
                });

        public static Flow<Message, Done, NotUsed> Create(IEnumerable<string> queuePaths, MessageQueueSettings queueSettings = null)
        {
            var settings = queueSettings ?? MessageQueueSettings.Default;
            var routingStrategy = settings.RoutingStrategy;

            var queues = new Lazy<IReadOnlyList<MessageQueue>>(() =>
                queuePaths.Select(path =>
                    new MessageQueue(path, settings.DenySharedReceive, settings.EnableConnectionCache, settings.AccessMode)
                    {
                        UseJournalQueue = settings.UseJournalQueue,
                        Formatter = settings.MessageFormatter
                    }).ToImmutableList());

            return Flow.Create<Message>()
                .SelectAsync(settings.Parallelism, async message =>
                {
                    foreach (var queue in routingStrategy.GetDestinations(typeof(Message).FullName, queues.Value))
                    {
                        await queue.SendAsync(message, queue.Transactional
                            ? MessageQueueTransactionType.Single
                            : MessageQueueTransactionType.None);
                    }

                    return Done.Instance;
                });
        }
    }
}