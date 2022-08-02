using System.Collections.Generic;
using System.Messaging;

namespace Akka.Streams.Msmq.Routing
{
    /// <summary>
    /// The first node routing strategy will send the outgoing message to the first destination.
    /// This is useful for dual destination fail-over systems.
    /// </summary>
    public sealed class FirstNodeRouting : IRoutingStrategy
    {
        public IEnumerable<MessageQueue> PickDestinations(string messageType, IReadOnlyList<MessageQueue> availableDestinations) =>
            new[] { availableDestinations[0] };
    }
}