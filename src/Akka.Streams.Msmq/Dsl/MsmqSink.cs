using System.Collections.Generic;
using System.Messaging;
using Akka.Streams.Dsl;

namespace Akka.Streams.Msmq
{
    public static class MsmqSink
    {
        /// <summary>
        /// Creates a <see cref="Sink"/> that sends to MSMQ for every stream element.
        /// </summary>
        /// <param name="queuePath">Path to the referenced <see cref="MessageQueue"/>.</param>
        /// <param name="settings">Settings to configure the <see cref="MessageQueue"/>.</param>
        public static Sink<Message, NotUsed> Create(string queuePath, MessageQueueSettings settings) =>
            Create(new[] { queuePath }, settings);

        /// <summary>
        /// Creates a <see cref="Sink"/> that sends to MSMQ for every stream element.
        /// </summary>
        /// <param name="queuePaths">Path(s) to the referenced <see cref="MessageQueue"/>(s).</param>
        /// <param name="settings">Settings to configure the <see cref="MessageQueue"/>.</param>
        public static Sink<Message, NotUsed> Create(IEnumerable<string> queuePaths, MessageQueueSettings settings) =>
            MsmqFlow.Create(queuePaths, settings).To(Sink.Ignore<Done>());
    }
}