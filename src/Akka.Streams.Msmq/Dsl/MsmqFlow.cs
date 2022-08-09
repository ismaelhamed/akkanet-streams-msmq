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
        /// <summary>
        /// Creates a <see cref="Flow"/> that sends to MSMQ for every stream element. 
        /// </summary>
        /// <param name="queuePaths">Path(s) to the referenced <see cref="MessageQueue"/>(s).</param>
        /// <param name="settings">Settings to configure the <see cref="MessageQueue"/>.</param>
        public static Flow<Message, Done, NotUsed> Create(IEnumerable<string> queuePaths, MessageQueueSettings settings) =>
            Flow.LazyInitAsync(() =>
            {
                var queues = queuePaths.Select(path => CreateMessageQueue(path, settings));
                var flow = Flow.Create<Message>()
                    .SelectAsync(settings.Parallelism, async message =>
                    {
                        var destinations = settings.RoutingStrategy.PickDestinations(typeof(Message).FullName, queues.ToImmutableArray()).ToArray();
                        var tasks = ArrayPool<Task>.Shared.Rent(destinations.Length);

                        for (int i = 0; i < destinations.Length; i++)
                        {
                            var queue = destinations[i];
                            tasks[i] = queue.SendAsync(message, queue.Transactional
                                ? MessageQueueTransactionType.Single
                                : MessageQueueTransactionType.None);
                        }

                        for (int i = destinations.Length; i < tasks.Length; i++)
                            tasks[i] = Task.FromResult(string.Empty);

                        await Task.WhenAll(tasks);
                        ArrayPool<Task>.Shared.Return(tasks);
                        return Done.Instance;
                    });

                return Task.FromResult(flow);
            })
            .MapMaterializedValue(_ => NotUsed.Instance);

        /// <summary>
        /// Creates a <see cref="Flow"/> that consumes stream elements of (<see cref="Message"/>, <see cref="MessageQueueTransaction"/>),
        /// send messages to MSMQ, and commit the associated <seealso cref="MessageQueueTransaction"/>.
        /// <para>
        /// Works with <see cref="MsmqSource.CreateWithContext"/> to receive and send messages within the context of the same
        /// <see cref="MessageQueueTransaction"/>, and provide the `Exactly-Once` semantic guarantee.
        /// </para>
        /// </summary>
        /// <param name="queuePaths">Path(s) to the referenced <see cref="MessageQueue"/>(s).</param>
        /// <param name="settings">Settings to configure the <see cref="MessageQueue"/>.</param>
        public static FlowWithContext<Message, MessageQueueTransaction, Message, MessageQueueTransaction, NotUsed> CreateWithContext(IEnumerable<string> queuePaths, MessageQueueSettings settings) =>
            FlowWithContext.From(Flow.LazyInitAsync(() =>
            {
                var queues = queuePaths.Select(path => CreateMessageQueue(path, settings));
                var flow = Flow.Create<(Message, MessageQueueTransaction)>()
                    .SelectAsync(settings.Parallelism, async tuple =>
                    {
                        var (message, trx) = tuple;

                        var destinations = settings.RoutingStrategy.PickDestinations(typeof(Message).FullName, queues.ToImmutableArray()).ToArray();
                        var tasks = ArrayPool<Task>.Shared.Rent(destinations.Length);

                        for (int i = 0; i < destinations.Length; i++)
                            tasks[i] = destinations[i].SendAsync(message, trx);

                        for (int i = destinations.Length; i < tasks.Length; i++)
                            tasks[i] = Task.FromResult(string.Empty);

                        await Task.WhenAll(tasks);
                        ArrayPool<Task>.Shared.Return(tasks);
                        return tuple;
                    });

                return Task.FromResult(flow);
            })
            .MapMaterializedValue(_ => NotUsed.Instance));

        private static MessageQueue CreateMessageQueue(string path, MessageQueueSettings settings) =>
            new MessageQueue(path, false, settings.EnableConnectionCache, QueueAccessMode.Send)
            {
                Formatter = settings.MessageFormatter
            };
    }
}