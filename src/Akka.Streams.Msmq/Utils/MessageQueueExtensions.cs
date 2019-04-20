using System.Threading.Tasks;

namespace System.Messaging
{
    public static class MessageQueueExtensions
    {
        public static async Task SendAsync(this MessageQueue queue, Message message)
        {
            // https://stackoverflow.com/a/45424928/465132
            await Task.Yield();
            queue.Send(message);
        }

        public static async Task SendAsync(this MessageQueue queue, Message message, MessageQueueTransaction transaction)
        {
            await Task.Yield();
            queue.Send(message, transaction);
        }

        public static Task<Message> ReceiveAsync(this MessageQueue queue) =>
            Task.Factory.FromAsync(queue.BeginReceive(TimeSpan.FromSeconds(10)), queue.EndReceive);

        public static Task<Message> ReceiveAsync(this MessageQueue queue, TimeSpan timeout) =>
            Task.Factory.FromAsync(queue.BeginReceive(timeout), queue.EndReceive);

        public static bool IsEmpty(this MessageQueue queue)
        {
            using (var enumerator = queue.GetMessageEnumerator2())
                return !enumerator.MoveNext();
        }
    }
}