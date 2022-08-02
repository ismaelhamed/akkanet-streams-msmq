using System;
using System.Buffers;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Messaging;
using System.Threading.Tasks;
using Akka.Streams.Dsl;

namespace Akka.Streams.Msmq
{
    public static class MsmqFlow
    {
        public static Flow<Message, Done, NotUsed> Create(IEnumerable<string> queuePaths, MessageQueueSettings queueSettings = null)
        {
            var settings = queueSettings ?? MessageQueueSettings.Default;

            var queues = new Lazy<IReadOnlyList<MessageQueue>>(() =>
                queuePaths.Select(path =>
                    new MessageQueue(path, false, settings.EnableConnectionCache, QueueAccessMode.Send)
                    {
                        Formatter = settings.MessageFormatter
                    }).ToImmutableList());

            return Flow.Create<Message>()
                // TODO: Paralellism should be divided among the nr. of destinations?!
                .SelectAsync(settings.MaxConcurrency, async message =>
                {
                    var destinations = settings.RoutingStrategy.PickDestinations(typeof(Message).FullName, queues.Value).ToArray();
                    var tasks = ArrayPool<Task>.Shared.Rent(destinations.Length);

                    for (int i = 0; i < destinations.Length; i++)
                    {
                        tasks[i] = destinations[i].SendAsync(message, settings.IsTransactional
                            ? MessageQueueTransactionType.Single
                            : MessageQueueTransactionType.None);
                    }

                    await Task.WhenAll(tasks);
                    ArrayPool<Task>.Shared.Return(tasks);
                    return Done.Instance;
                });
        }
    }
}