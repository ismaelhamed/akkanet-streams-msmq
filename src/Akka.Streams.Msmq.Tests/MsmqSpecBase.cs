using System.Messaging;
using Akka.Actor;
using Xunit.Abstractions;

namespace Akka.Streams.Msmq.Tests
{
    // https://github.com/AkkaNetContrib/Alpakka/blob/dev/Azure/src/Akka.Streams.Azure.StorageQueue.Tests/QueueSpecBase.cs
    public abstract class MsmqSpecBase : Akka.TestKit.Xunit2.TestKit
    {
        protected ActorMaterializer Materializer { get; }
        public MessageQueue Queue { get; }

        protected MsmqSpecBase(MessageQueueFixture fixture, ITestOutputHelper output)
            : base((ActorSystem)null, output)
        {
            Materializer = Sys.Materializer();
            Queue = new MessageQueue(@".\Private$\MsmqSpecQueue")
            {
                Formatter = fixture.Formatter
            };
        }

        protected void EnsureQueueExists(string queuePath, bool transactional = true)
        {
            if (!MessageQueue.Exists(queuePath))
                MessageQueue.Create(queuePath, transactional);
        }

        protected void EnsureQueueIsDeleted(string queuePath)
        {
            if (MessageQueue.Exists(queuePath))
                MessageQueue.Delete(queuePath);
        }

        public new void Dispose() => Queue.Purge();
    }
}
