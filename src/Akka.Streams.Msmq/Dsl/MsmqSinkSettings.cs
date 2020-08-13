using System.Messaging;
//-----------------------------------------------------------------------
// <copyright file="MsmqFlow.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2020 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2020 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

namespace Akka.Streams.Msmq
{
    public sealed class MsmqSinkSettings
    {
        /// <summary>
        /// Gets or sets a value that indicates whether this <see cref="MessageQueue"/> has exclusive access to receive messages from the queue.
        /// </summary>
        public bool DenySharedReceive { get; set; }

        /// <summary>
        /// Gets or sets a value that indicates whether a cache of connections will be maintained by the application.
        /// </summary>
        public bool EnableConnectionCache { get; set; }

        /// <summary>
        /// Gets or sets the formatter used to serialize an object into or deserialize an object from the body of a message read from or written to the queue.
        /// The default is <see cref="XmlMessageFormatter"/>.
        /// </summary>
        public IMessageFormatter Formatter { get; set; }

        /// <summary>
        /// Gets or sets a value that indicates whether received messages are copied to the journal queue.
        /// </summary>
        public bool UseJournalQueue { get; set; }

        public MsmqSinkSettings() => Formatter = new XmlMessageFormatter();
    }
}
