using System.Messaging;

namespace Akka.Streams.Msmq.Tests
{
    // https://github.com/AkkaNetContrib/Alpakka/blob/dev/Azure/src/Akka.Streams.Azure.StorageQueue.Tests/QueueSpecBase.cs
    public abstract class MsmqSpecBase : Akka.TestKit.Xunit2.TestKit
    {
        protected ActorMaterializer Materializer { get; }

        protected MsmqSpecBase()
        {
            Materializer = Sys.Materializer();
        }

        protected void EnsureQueueExists(string queuePath, bool transactional = true)
        {
            if (!MessageQueue.Exists(queuePath))
            {
                MessageQueue.Create(queuePath, transactional);
            }
        }

        protected void EnsureQueueIsDeleted(string queuePath)
        {
            if (MessageQueue.Exists(queuePath))
            {
                MessageQueue.Delete(queuePath);
            }
        }
    }
}
