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
        //public static Sink<Message, NotUsed> Create(string queuePath, MessageQueueSettings settings = null) =>
        //    Create(new[] { queuePath }, settings);

        //public static Sink<Message, NotUsed> Create(IEnumerable<string> queuePaths, MessageQueueSettings settings = null) =>
        //    Flow.Create<Message>()
        //        .Via(MsmqFlow.Create(queuePaths, settings))
        //        .Log("Sink.Create")
        //        .To(Sink.Ignore<Done>());

        public static Sink<Message, Task> Create(string queuePath, MessageQueueSettings settings = null) =>
            Create(new[] { queuePath }, settings);

        public static Sink<Message, Task> Create(IEnumerable<string> queuePaths, MessageQueueSettings settings = null) =>
            MsmqFlow.Create(queuePaths, settings).ToMaterialized(Sink.Ignore<Done>(), Keep.Right);
    }
}