using System.Collections.Generic;
using System.Messaging;

namespace Akka.Streams.Msmq.Routing
{
    /// <summary>
    /// The broadcast routing strategy will send the outgoing message to all registered destination queues.
    /// </summary>
    public sealed class BroadcastRouting : IRoutingStrategy
    {
        public IEnumerable<MessageQueue> PickDestinations(string messageType, IReadOnlyList<MessageQueue> availableDestinations) =>
            availableDestinations;
    }
}