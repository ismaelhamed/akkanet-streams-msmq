using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Messaging;
using System.Threading;

namespace Akka.Streams.Msmq.Routing
{
    /// <summary>
    /// The Round Robin routing strategy will send the outgoing message to the next destination queue, in a sequential order.
    /// </summary>
    public class RoundRobinRouting : IRoutingStrategy
    {
        private readonly ConcurrentDictionary<string, int> indexes = new ConcurrentDictionary<string, int>();

        /// <summary>
        /// Picks the next receiver's destination queue in the collection to receive the message.
        /// </summary>
        /// <param name="messageType">TBD</param>
        /// <param name="destinations">List of receiver's message queues</param>
        public IEnumerable<MessageQueue> GetDestinations(string messageType, IReadOnlyList<MessageQueue> destinations)
        {
            if (!destinations.Any())
                throw new ArgumentNullException(nameof(destinations));

            // Start with -1 so the current index will be at 0 after the first increment.
            var current = indexes.GetOrAdd(messageType, _ => -1);

            var next = indexes[messageType] = Interlocked.Increment(ref current);
            var index = ( next & int.MaxValue ) % destinations.Count;
            yield return destinations[index < 0 ? destinations.Count + index - 1 : index];
        }
    }
}