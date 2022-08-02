using System;
using System.Messaging;
using Akka.Streams.Msmq.Routing;

namespace Akka.Streams.Msmq
{
    public class MessageQueueSettings
    {
        /// <summary>
        /// Gets a value that indicates whether this <see cref="MessageQueue"/> has exclusive access to receive messages from the queue.
        /// </summary>
        public bool DenySharedReceive { get; }

        /// <summary>
        /// Gets a value that indicates whether a cache of connections will be maintained by the application.
        /// </summary>
        public bool EnableConnectionCache { get; }

        /// <summary>
        /// Gets a value that indicates whether received messages are copied to the journal queue.
        /// </summary>
        public bool UseJournalQueue { get; }

        /// <summary>
        /// Gets the access mode for a <see cref="MessageQueue" /> at creation time.
        /// </summary>
        public QueueAccessMode AccessMode { get; }

        /// <summary>
        /// Gets the properties that are retrieved when peeking or receiving messages from a <see cref="MessageQueue"/>.
        /// </summary>
        public MessagePropertyFilter MessagePropertyFilter { get; }

        /// <summary>
        /// Gets the formatter used to serialize an object into or deserialize an object from the body of a message read from or written to the queue.
        /// The default is <see cref="XmlMessageFormatter"/>.
        /// </summary>
        public IMessageFormatter MessageFormatter { get; }

        /// <summary>
        /// Gets the routing strategy configured to route messages between destination queues. The <see cref="BroadcastRouting"/> strategy is used by default.
        /// </summary>
        public IRoutingStrategy RoutingStrategy { get; }

        /// <summary>
        /// Gets the max. number of concurrent `send` operations that a stream can process at any given time.
        /// </summary>
        public int MaxConcurrency { get; }

        /// <summary>
        /// Gets a value that indicates whether the queue accepts only transactions.
        /// </summary>
        public bool IsTransactional { get; }

        /// <summary>
        /// Default <see cref="MessageQueueSettings"/>
        /// </summary>
        public static MessageQueueSettings Default = new MessageQueueSettings(
            false,
            false,
            false,
            QueueAccessMode.SendAndReceive);

        public MessageQueueSettings(
            bool denySharedReceive,
            bool enableCache,
            bool useJournalQueue,
            QueueAccessMode accessMode,
            MessagePropertyFilter messagePropertyFilter = null,
            IMessageFormatter messageFormatter = null,
            IRoutingStrategy routingStrategy = null,
            int? maxConcurrency = null,
            bool? isTransactional = null)
        {
            DenySharedReceive = denySharedReceive;
            EnableConnectionCache = enableCache;
            UseJournalQueue = useJournalQueue;
            AccessMode = accessMode;
            MessagePropertyFilter = messagePropertyFilter ?? DefaultReadPropertyFilter;
            MessageFormatter = messageFormatter ?? new XmlMessageFormatter(new[] { "System.String,mscorlib" });
            RoutingStrategy = routingStrategy ?? new BroadcastRouting();
            MaxConcurrency = maxConcurrency ?? Math.Max(2, Environment.ProcessorCount);
            IsTransactional = isTransactional ?? false;
        }

        public MessageQueueSettings WithMessagePropertyFilter(MessagePropertyFilter messagePropertyFilter) =>
            Copy(messagePropertyFilter: messagePropertyFilter);

        public MessageQueueSettings WithFormatter(IMessageFormatter messageFormatter) =>
            Copy(messageFormatter: messageFormatter);

        public MessageQueueSettings WithRoutingStrategy(IRoutingStrategy routingStrategy) =>
            Copy(routingStrategy: routingStrategy);

        public MessageQueueSettings WithMaxConcurrency(int maxConcurrency) =>
            Copy(maxConcurrency: maxConcurrency);

        public MessageQueueSettings WithIsTransactional(bool isTransactional) =>
            Copy(isTransactional: isTransactional);

        private MessageQueueSettings Copy(
            bool? sharedModeDenyReceive = null,
            bool? enableCache = null,
            bool? useJournalQueue = null,
            QueueAccessMode? accessMode = null,
            MessagePropertyFilter messagePropertyFilter = null,
            IMessageFormatter messageFormatter = null,
            IRoutingStrategy routingStrategy = null,
            int? maxConcurrency = null,
            bool? isTransactional = null) =>
            new MessageQueueSettings(
                sharedModeDenyReceive ?? DenySharedReceive,
                enableCache ?? EnableConnectionCache,
                useJournalQueue ?? UseJournalQueue,
                accessMode ?? AccessMode,
                messagePropertyFilter ?? MessagePropertyFilter,
                messageFormatter ?? MessageFormatter,
                routingStrategy ?? RoutingStrategy,
                maxConcurrency ?? MaxConcurrency,
                isTransactional ?? IsTransactional);

        private static MessagePropertyFilter DefaultReadPropertyFilter => new MessagePropertyFilter
        {
            Id = true,
            AppSpecific = true,
            Body = true,
            CorrelationId = true,
            Extension = true,
            Recoverable = true,
            ResponseQueue = true,
            TimeToBeReceived = true
        };
    }
}