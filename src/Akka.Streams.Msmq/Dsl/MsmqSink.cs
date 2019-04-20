using System.Messaging;
using System.Threading.Tasks;
using Akka.Streams.Dsl;

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

        public MsmqSinkSettings()
        {
            Formatter = new XmlMessageFormatter();
        }
    }

    public class MsmqSink
    {
        public static Sink<Message, Task<Done>> Create(MessageQueue queue)
        {
            return Sink.FromGraph(new MsmqSinkStage(queue));
        }
    }
}
