using System.Threading.Tasks;
using Akka.Util;

namespace System.Messaging
{
    public static class MessageQueueExtensions
    {
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
                .ContinueWith(t => t.IsFaulted || t.IsCanceled ? Option<Message>.None : t.Result);

        public static bool IsEmpty(this MessageQueue queue)
        {
            using (var enumerator = queue.GetMessageEnumerator2())
                return !enumerator.MoveNext();
        }
    }

    public interface IMessageQueue
    {
        Task SendAsync(Message message);
        Task SendAsync(Message message, MessageQueueTransactionType transactionType);
        Task SendAsync(Message message, MessageQueueTransaction transaction);
        Task<Message> ReceiveAsync();
        Task<Message> ReceiveAsync(TimeSpan timeout);
    }

    // public class MsmqQueue : IMessageQueue
    // {
    //     private readonly MessageQueue _queue;
    //
    //     public MsmqQueue(string path) => _queue = new MessageQueue(path);
    //
    //     public Task SendAsync(Message message) =>
    //         _queue.SendAsync(message);
    //
    //     public Task SendAsync(Message message, MessageQueueTransactionType transactionType) =>
    //         _queue.SendAsync(message, transactionType);
    //
    //     public Task SendAsync(Message message, MessageQueueTransaction transaction) =>
    //         _queue.SendAsync(message, transaction);
    //
    //     public Task<Message> ReceiveAsync(TimeSpan? timeout = null) => timeout == null
    //         ? _queue.ReceiveAsync()
    //         : _queue.ReceiveAsync(timeout.Value);
    // }
}