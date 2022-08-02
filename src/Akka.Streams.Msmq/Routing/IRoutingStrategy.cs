using System.Collections.Generic;
using System.Messaging;

namespace Akka.Streams.Msmq.Routing
{
    /// <summary>
    /// A routing strategy to route messages between destination queues.
    /// </summary>
    public interface IRoutingStrategy
    {
        IEnumerable<MessageQueue> PickDestinations(string messageType, IReadOnlyList<MessageQueue> availableDestinations);
    }
}