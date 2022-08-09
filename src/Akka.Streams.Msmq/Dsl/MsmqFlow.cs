using System.Messaging;
using System.Threading.Tasks;
using Akka.Streams.Dsl;

namespace Akka.Streams.Msmq
{
    public static class MsmqFlow
    {
        /// <summary>
        /// Creates a <see cref="Flow"/> that sends to MSMQ for every stream element and then pass it on.
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
        public static Flow<Message, Message, NotUsed> Default(MessageQueueSettings settings, string queuePath) =>
            Flow.LazyInitAsync(() =>
            {
                var queue = ResolveMessageQueue(queuePath, settings);
                var flow = Flow.Create<Message>()
                    .SelectAsync(settings.Parallelism, async message =>
                    {
                        await queue.SendAsync(message, queue.Transactional
                            ? MessageQueueTransactionType.Single
                            : MessageQueueTransactionType.None);

                        return message;
                    });

                return Task.FromResult(flow);
            })
            .MapMaterializedValue(_ => NotUsed.Instance);

        /// <summary>
        /// Creates a <see cref="Flow"/> that consumes stream elements of (<see cref="Message"/>, <see cref="MessageQueueTransaction"/>)
        /// and sends messages to MSMQ in the context of the associated <seealso cref="MessageQueueTransaction"/>.
        /// <para>
        /// This flow is intended to be used with <see cref="MsmqSource.WithTransactionContext"/> to receive and send messages within the context of the same
        /// <see cref="MessageQueueTransaction"/>, providing `Exactly-Once` semantic guarantee.
        /// </para>
        /// </summary>
        /// <param name="settings">Settings to configure the <see cref="MessageQueue"/>.</param> 
        /// <param name="queuePath">The location of the queue referenced by the underlying <see cref="MessageQueue"/>.
        /// The syntax for <paramref name="queuePath"/> depends on the type of queue it points to, as shown in the following
        /// <see href="https://learn.microsoft.com/en-us/dotnet/api/system.messaging.messagequeue.path">table</see>.
        /// </param>
        public static FlowWithContext<Message, MessageQueueTransaction, Message, MessageQueueTransaction, NotUsed> WithTransactionContext(MessageQueueSettings settings, string queuePath) =>
            FlowWithContext.From(
                Flow.LazyInitAsync(() =>
                {
                    var queue = ResolveMessageQueue(queuePath, settings);
                    var flow = Flow.Create<(Message, MessageQueueTransaction)>()
                        .SelectAsync(settings.Parallelism, async tuple =>
                        {
                            var (message, trx) = tuple;
                            await queue.SendAsync(message, trx);
                            return tuple;
                        });

                    return Task.FromResult(flow);
                })
                .MapMaterializedValue(_ => NotUsed.Instance));

        private static MessageQueue ResolveMessageQueue(string path, MessageQueueSettings settings) =>
            new MessageQueue(path, false, settings.EnableConnectionCache, QueueAccessMode.Send)
            {
                Formatter = settings.MessageFormatter
            };
    }
}