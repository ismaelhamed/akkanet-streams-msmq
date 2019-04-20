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
        /// Creates a <see cref="Sink{TIn,TMat}"/> for MSMQ
        /// </summary>
        /// <param name="queue">The message queue</param>
        /// <returns>The <see cref="Sink{TIn,TMat}"/> for MSMQ</returns>
        public static Sink<Message, Task<Done>> Create(MessageQueue queue) => Sink.FromGraph(new MsmqSinkStage(queue));
        //public static Sink<IEnumerable<Message>, Task<Done>> Create(MessageQueue queue) => Sink.FromGraph(new MsmqSinkStage(queue));
    }
}
