using System;
using System.Messaging;

namespace Akka.Streams.Msmq.Tests
{
    public class MessageQueueFixture : IDisposable
    {
        public string QueuePath { get; } = @".\Private$\MsmqSpecQueue";
        public MessageQueue Queue { get; }

        public MessageQueueFixture()
        {
            Queue = new MessageQueue(QueuePath, QueueAccessMode.SendAndReceive);
        }

        public void Dispose()
        {
            // ... clean up test data from msmq ...
            Queue.Purge();
        }
    }
}