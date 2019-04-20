using System.Threading.Tasks;

namespace System.Messaging
{
    public static class MessageQueueExtensions
    {
        public static Task<Message> ReceiveAsync(this MessageQueue queue, TimeSpan timeout)
        {
            return Task.Factory.FromAsync(queue.BeginReceive(timeout), queue.EndReceive);
        }

        public static bool IsEmpty(this MessageQueue queue)
        {
            using (var enumerator = queue.GetMessageEnumerator2())
                return !enumerator.MoveNext();
        }
    }
}