using System.Collections.Generic;
using System.Messaging;
using System.Threading.Tasks;
using Akka.Streams.Dsl;

namespace Akka.Streams.Msmq
{
    /// <summary>
    /// A container for factory methods used to build Akka.NET Streams sinks to MSMQ.
    /// Sinks can be used to send data to the Message Queuing server.
    /// </summary>
    public static class MsmqSink
    {
        /// <summary>
        /// Creates a <see cref="Sink{TIn,TMat}"/> to send messages to an MSMQ queue
        /// </summary>
        /// <param name="queue">The message queue</param>
        /// <param name="settings">TBD</param>
        /// <returns>The <see cref="Sink{TIn,TMat}"/> for the MSMQ queue</returns>
        internal static Sink<Message, Task> Create(MessageQueue queue, MessageQueueSettings settings) =>
            MsmqFlow.Default(queue, settings).ToMaterialized(Sink.Ignore<Done>(), Keep.Right);

        public static Sink<Message, Task> Create(string queuePath, MessageQueueSettings settings = null) =>
            Create(new[] {queuePath}, settings);

        public static Sink<Message, Task> Create(IEnumerable<string> queuePaths, MessageQueueSettings settings = null) =>
            MsmqFlow.Create(queuePaths, settings).ToMaterialized(Sink.Ignore<Done>(), Keep.Right);
    }
}