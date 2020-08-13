using System.Messaging;

namespace Akka.Streams.Msmq.Tests
{
    public class MessageQueueFixture
    {
        public string QueuePath { get; } = @".\Private$\MsmqSpecQueue";
        public IMessageFormatter Formatter { get; } = new XmlMessageFormatter(new[] {typeof(string)});
    }
}