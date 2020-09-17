using System.Collections.Generic;
using System.Messaging;

namespace Akka.Streams.Msmq.Routing
{
    /// <summary>
    /// The broadcast routing strategy will send the outgoing message to all registered destination queues.
    /// </summary>
    public class BroadcastRouting : IRoutingStrategy
    {
        public IEnumerable<MessageQueue> GetDestinations(string messageType, IReadOnlyList<MessageQueue> destinations) =>
            destinations;
    }
}