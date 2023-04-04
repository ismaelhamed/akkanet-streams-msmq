// Copyright (c) 2023 Ismael Hamed. All rights reserved.
// See LICENSE file in the root folder for full license information.

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
                }, TaskScheduler.Default);

        private static Exception TryUnwrapException(Exception e)
        {
            if (!(e is AggregateException aggregateException)) return e;
            aggregateException = aggregateException.Flatten();
            return aggregateException.InnerExceptions.Count == 1 ? aggregateException.InnerExceptions[0] : e;
        }
    }
}