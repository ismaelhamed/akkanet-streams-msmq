using System.Threading.Tasks;
using Akka.Util;

namespace System.Messaging
{
    public static class MessageQueueExtensions
    {
        public static bool IsEmpty(this MessageQueue queue)
        {
            using var enumerator = queue.GetMessageEnumerator2();
            return !enumerator.MoveNext();
        }

        public static Task SendAsync(this MessageQueue queue, Message message) =>
            SendAsync(queue, message, queue.Transactional ? MessageQueueTransactionType.Single : MessageQueueTransactionType.None);

        public static async Task SendAsync(this MessageQueue queue, Message message, MessageQueueTransactionType transactionType)
        {
            // Send is blocking although it is only writing to a local queue
            await Task.Yield();
            queue.Send(message, transactionType);
        }

        public static async Task SendAsync(this MessageQueue queue, Message message, MessageQueueTransaction transaction)
        {
            // Send is blocking although it is only writing to a local queue
            await Task.Yield();
            queue.Send(message, transaction);
        }

        /// <summary>
        /// Initiates a synchronous receive operation that has a default timeout of 10 milliseconds.
        /// The operation is not complete until either a message becomes available in the queue or the timeout occurs.
        /// </summary>
        /// <param name="queue">The Message Queue.</param>
        /// <remarks>Do not use this call with transactional queues.</remarks>
        internal static Option<Message> TryReceive(this MessageQueue queue) =>
            TryReceive(queue, TimeSpan.FromMilliseconds(10));

        /// <summary>
        /// Initiates a synchronous receive operation that has a specified timeout.
        /// The operation is not complete until either a message becomes available in the queue or the timeout occurs.
        /// </summary>
        /// <param name="queue">The Message Queue.</param>
        /// <param name="timeout">A <see cref="TimeSpan"/> that indicates the interval of time to wait for a message to become available.</param>
        /// <remarks>Do not use this call with transactional queues.</remarks>
        internal static Option<Message> TryReceive(this MessageQueue queue, TimeSpan timeout)
        {
            try
            {
                return new Option<Message>(queue.Receive(timeout, queue.Transactional
                    ? MessageQueueTransactionType.Single
                    : MessageQueueTransactionType.None));
            }
            catch (MessageQueueException ex) when (ex.MessageQueueErrorCode == MessageQueueErrorCode.IOTimeout)
            {
                return Option<Message>.None;
            }
        }

        public static Task<Option<Message>> ReceiveAsync(this MessageQueue queue) =>
            ReceiveAsync(queue, TimeSpan.FromMilliseconds(10));

        public static Task<Option<Message>> ReceiveAsync(this MessageQueue queue, TimeSpan timeout) =>
            Task.Factory.FromAsync(queue.BeginReceive(timeout), queue.EndReceive)
                .ContinueWith(t =>
                {
                    var innerException = TryUnwrapException(t.Exception);
                    switch (innerException)
                    {
                        case MessageQueueException ex when ex.MessageQueueErrorCode == MessageQueueErrorCode.IOTimeout:
                            return Option<Message>.None;
                        case null when !t.IsFaulted && !t.IsCanceled:
                            return new Option<Message>(t.Result);
                        case null:
                            return Option<Message>.None;
                        default:
                            queue.Close(); // ideally we would only do this in case of MessageQueueErrorCode.StaleHandle
                            throw innerException;
                    }
                });

        private static Exception TryUnwrapException(Exception e)
        {
            if (!(e is AggregateException aggregateException)) return e;
            aggregateException = aggregateException.Flatten();
            return aggregateException.InnerExceptions.Count == 1 ? aggregateException.InnerExceptions[0] : e;
        }
    }
}