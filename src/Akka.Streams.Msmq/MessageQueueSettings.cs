// Copyright (c) 2023 Ismael Hamed. All rights reserved.
// See LICENSE file in the root folder for full license information.

using System;
using System.Messaging;

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
        /// Gets a value that indicates the time to wait until a new message is available for inspection. Defaults to 10ms.
        /// </summary>
        public TimeSpan WaitTimeout { get; }

        /// <summary>
        /// Gets the max. number of concurrent `send` or `receive` operations that a stream can process at any given time. Defaults to 1.
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

        protected MessageQueueSettings(
            bool denySharedReceive,
            bool enableCache,
            bool useJournalQueue,
            QueueAccessMode accessMode,
            MessagePropertyFilter messagePropertyFilter = null,
            IMessageFormatter messageFormatter = null,
            TimeSpan? waitTimeout = null,
            int? parallelism = null)
        {
            DenySharedReceive = denySharedReceive;
            EnableConnectionCache = enableCache;
            UseJournalQueue = useJournalQueue;
            AccessMode = accessMode;
            MessagePropertyFilter = messagePropertyFilter ?? DefaultReadPropertyFilter;
            MessageFormatter = messageFormatter ?? new XmlMessageFormatter(new[] { "System.String,mscorlib" });
            WaitTimeout = waitTimeout ?? TimeSpan.FromMilliseconds(10);
            Parallelism = parallelism ?? 1;
        }

        public MessageQueueSettings WithMessagePropertyFilter(MessagePropertyFilter messagePropertyFilter) =>
            Copy(messagePropertyFilter: messagePropertyFilter);

        public MessageQueueSettings WithFormatter(IMessageFormatter messageFormatter) =>
            Copy(messageFormatter: messageFormatter);

        public MessageQueueSettings WithWaitTimeout(TimeSpan waitTimeout) =>
            Copy(waitTimeout: waitTimeout);

        public MessageQueueSettings WithParallelism(int parallelism) =>
            Copy(maxConcurrency: parallelism);

        private MessageQueueSettings Copy(
            bool? sharedModeDenyReceive = null,
            bool? enableCache = null,
            bool? useJournalQueue = null,
            QueueAccessMode? accessMode = null,
            MessagePropertyFilter messagePropertyFilter = null,
            IMessageFormatter messageFormatter = null,
            TimeSpan? waitTimeout = null,
            int? maxConcurrency = null) =>
            new MessageQueueSettings(
                sharedModeDenyReceive ?? DenySharedReceive,
                enableCache ?? EnableConnectionCache,
                useJournalQueue ?? UseJournalQueue,
                accessMode ?? AccessMode,
                messagePropertyFilter ?? MessagePropertyFilter,
                messageFormatter ?? MessageFormatter,
                waitTimeout ?? WaitTimeout,
                maxConcurrency ?? Parallelism);

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