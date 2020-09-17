using System.Messaging;
using Akka.Streams.Msmq.Routing;

namespace Akka.Streams.Msmq
{
    public sealed class MessageQueueSettings
    {
        /// <summary>
        /// Gets or sets a value that indicates whether this <see cref="MessageQueue"/> has exclusive access to receive messages from the queue.
        /// </summary>
        public bool DenySharedReceive { get; }

        /// <summary>
        /// Gets or sets a value that indicates whether a cache of connections will be maintained by the application.
        /// </summary>
        public bool EnableConnectionCache { get; }

        /// <summary>
        /// Gets or sets a value that indicates whether received messages are copied to the journal queue.
        /// </summary>
        public bool UseJournalQueue { get; }

        /// <summary>
        /// Specifies the access mode for a <see cref="MessageQueue" /> at creation time.
        /// </summary>
        public QueueAccessMode AccessMode { get; }

        /// <summary>
        /// Gets or sets the formatter used to serialize an object into or deserialize an object from the body of a message read from or written to the queue.
        /// The default is <see cref="XmlMessageFormatter"/>.
        /// </summary>
        public IMessageFormatter MessageFormatter { get; }

        /// <summary>
        /// A routing strategy to route messages between destination queues. The broadcast strategy is used by default.
        /// </summary>
        public IRoutingStrategy RoutingStrategy { get; }

        /// <summary>
        /// TBD
        /// </summary>
        public int Parallelism { get; }

        /// <summary>
        /// Default <see cref="MessageQueueSettings"/>
        /// </summary>
        public static MessageQueueSettings Default = new MessageQueueSettings(
            false,
            false,
            false,
            QueueAccessMode.SendAndReceive);

        public MessageQueueSettings(
            bool sharedModeDenyReceive,
            bool enableCache,
            bool useJournalQueue,
            QueueAccessMode accessMode,
            IMessageFormatter messageFormatter = null,
            IRoutingStrategy routingStrategy = null,
            int parallelism = 1)
        {
            DenySharedReceive = sharedModeDenyReceive;
            EnableConnectionCache = enableCache;
            UseJournalQueue = useJournalQueue;
            AccessMode = accessMode;
            MessageFormatter = messageFormatter ?? new XmlMessageFormatter();
            RoutingStrategy = routingStrategy ?? new BroadcastRouting();
            Parallelism = parallelism;
        }

        public MessageQueueSettings WithFormatter(IMessageFormatter messageFormatter) =>
            Copy(messageFormatter: messageFormatter);

        public MessageQueueSettings WithRoutingStrategy(IRoutingStrategy routingStrategy) =>
            Copy(routingStrategy: routingStrategy);

        public MessageQueueSettings WithParallelism(int parallelism) =>
            Copy(parallelism: parallelism);

        private MessageQueueSettings Copy(
            bool? sharedModeDenyReceive = null,
            bool? enableCache = null,
            bool? useJournalQueue = null,
            QueueAccessMode? accessMode = null,
            IMessageFormatter messageFormatter = null,
            IRoutingStrategy routingStrategy = null,
            int? parallelism = null) =>
            new MessageQueueSettings(
                sharedModeDenyReceive ?? DenySharedReceive,
                enableCache ?? EnableConnectionCache,
                useJournalQueue ?? UseJournalQueue,
                accessMode ?? AccessMode,
                messageFormatter ?? MessageFormatter,
                routingStrategy ?? RoutingStrategy,
                parallelism ?? Parallelism);
    }
}