//-----------------------------------------------------------------------
// <copyright file="MsmqFlow.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2020 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2020 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Messaging;
using System.Threading;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Streams.Dsl;
using Akka.Util.Internal;

namespace Akka.Streams.Msmq
{
    /// <summary>
    /// A container for factory methods used to build Akka.NET Streams sinks to MSMQ.
    /// Sinks can be used to send data to the Message Queuing server.
    /// </summary>
    public static class MsmqSink
    {
        // private static MessageQueue queue;
        // private static readonly Func<string, MessageQueue> messageQueueFactory = queuePath =>
        //     new MessageQueue(queuePath)
        //     {
        //         Formatter = new XmlMessageFormatter(new[] {typeof(string)})
        //     };

        /// <summary>
        /// Creates a <see cref="Sink{TIn,TMat}"/> to send messages to an MSMQ queue
        /// </summary>
        /// <param name="queue">The message queue</param>
        /// <returns>The <see cref="Sink{TIn,TMat}"/> for the MSMQ queue</returns>
        public static Sink<Message, Task> Default(MessageQueue queue) =>
            MsmqFlow.Default(queue).ToMaterialized(Sink.Ignore<Done>(), Keep.Right);

        // public static Sink<Message, Task> Default(string queuePath) =>
        //     MsmqFlow.Default(queuePath).ToMaterialized(Sink.Ignore<Done>(), Keep.Right);

        public static Sink<Message, Task> Default(string queuePath, MsmqSinkSettings settings = null)
        {
            var queue = new MessageQueue(queuePath)
            {
                Formatter = new XmlMessageFormatter(new[] {typeof(string)})
            };

            return Flow.FromFunction<Message, Task<Done>>(async message =>
                {
                    await queue.SendAsync(message).ConfigureAwait(false);
                    return Done.Instance;
                })
                .ToMaterialized(Sink.Ignore<Task<Done>>(), Keep.Right);
        }

        public static Sink<Message, Task> Default(string[] queuePaths, MsmqSinkSettings settings = null)
        {
            var destinations = queuePaths.Select(path => new MessageQueue(path) {Formatter = new XmlMessageFormatter(new[] {typeof(string)})}).ToList();
            var roundRobinExchange = new RoundRobinExchange();

            return Flow.FromFunction<Message, Done>(message =>
                {
                    var destination = roundRobinExchange.GetDestination(typeof(Message).FullName, destinations);
                    destination.Send(message, destination.Transactional ? MessageQueueTransactionType.Single : MessageQueueTransactionType.None);
                    return Done.Instance;
                })
                .ToMaterialized(Sink.Ignore<Done>(), Keep.Right);
        }

        // public static Sink<Message, Task> Default(string[] queuePaths, MsmqSinkSettings settings = null)
        // {
        //     var destinations = queuePaths.Select(path => new MessageQueue(path) {Formatter = new XmlMessageFormatter(new[] {typeof(string)})}).ToList();
        //     var balancer = new RoundRobinBalancer();
        //
        //     return Flow.FromFunction<Message, Task<Done>>(async message =>
        //         {
        //             try
        //             {
        //                 var destination = balancer.GetDestinationToSend(typeof(string).FullName, destinations);
        //                 await destination.SendAsync(message);
        //                 return Done.Instance;
        //             }
        //             catch (Exception e)
        //             {
        //                 Console.WriteLine(e);
        //                 return null;
        //             }
        //         })
        //         .Log("Log")
        //         .ToMaterialized(Sink.Ignore<Task<Done>>(), Keep.Right);
        // }
    }

    public interface IExchangeStrategy
    {
        MessageQueue GetDestination(string messageType, List<MessageQueue> destinations);
    }

    public class RoundRobinExchange : IExchangeStrategy
    {
        private readonly ConcurrentDictionary<string, int> indexes = new ConcurrentDictionary<string, int>();

        /// <summary>
        /// Picks the next receiver's <see cref="MessageQueue"/> in the collection to receive the message.
        /// </summary>
        /// <param name="messageType"></param>
        /// <param name="destinations">List of receiver's message queues</param>
        public MessageQueue GetDestination(string messageType, List<MessageQueue> destinations)
        {
            if (!destinations.Any())
                return null;

            // Start with -1 so the current index will be at 0 after the first increment.
            var current = indexes.GetOrAdd(messageType, _ => -1);

            var next = indexes[messageType] = Interlocked.Increment(ref current);
            var index = ( next & int.MaxValue ) % destinations.Count;
            return destinations[index < 0 ? destinations.Count + index - 1 : index];
        }
    }
}