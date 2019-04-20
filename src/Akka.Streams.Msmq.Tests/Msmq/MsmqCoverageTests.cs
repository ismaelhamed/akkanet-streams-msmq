using System;
using System.Diagnostics;
using System.IO;
using System.Messaging;
using System.Text;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Streams.Msmq.Tests
{
    [Serializable]
    public class SomeMessage
    {
        public int SomeProperty { get; set; }
    }

    public class MsmqCoverageTests
    {
        private readonly ITestOutputHelper output;

        public MsmqCoverageTests(ITestOutputHelper output) => this.output = output;

        [Fact]
        public void SendMessagesWithXmlFormatter()
        {
            const string queuePath = @".\Private$\MsmqSpecQueue";
            EnsureQueueExists(queuePath);

            var messageBody = new SomeMessage() { SomeProperty = 1234 };

            using (var queue = new MessageQueue(queuePath, QueueAccessMode.Send))
            using (var message = new Message())
            {
                message.Body = messageBody;
                queue.Send(message);
            }
        }

        [Fact]
        public void SendMessagesWrappedInASharedTransaction()
        {
            const string queuePath = @".\Private$\MsmqSpecQueueWithTrx";
            EnsureQueueExists(queuePath);

            var messageBodys = new[]
            {
                "{\"Value\":\"1\"}",
                "{\"Value\":\"2\"}",
                "{\"Value\":\"3\"}"
            };

            using (var transaction = new MessageQueueTransaction())
            {
                transaction.Begin();

                using (var queue = new MessageQueue(queuePath, QueueAccessMode.Send))
                {
                    var count = 0;
                    foreach (var messageBody in messageBodys)
                    {
                        if (count > 1) throw new Exception("Testing transaction");

                        using (var message = new Message())
                        {
                            message.BodyStream = new MemoryStream(Encoding.UTF8.GetBytes(messageBody));
                            queue.Send(message, transaction);
                        }
                        count++;
                    }
                }

                transaction.Commit();
            }
        }

        [Fact]
        public void SendMessagesWithSingleTransaction()
        {
            const string queuePath = @".\Private$\MsmqSpecQueue";
            EnsureQueueExists(queuePath);

            var messageBodys = new[]
            {
                "{\"Value\":\"1\"}",
                "{\"Value\":\"2\"}",
                "{\"Value\":\"3\"}"
            };

            using (var queue = new MessageQueue(queuePath, QueueAccessMode.Send))
            {
                var count = 0;
                foreach (var messageBody in messageBodys)
                {
                    if (count > 1) throw new Exception("Testing transaction");

                    using (var message = new Message())
                    {
                        message.BodyStream = new MemoryStream(Encoding.UTF8.GetBytes(messageBody));
                        queue.Send(message, queue.Transactional ? MessageQueueTransactionType.Single : MessageQueueTransactionType.None);
                    }
                    count++;
                }
            }
        }

        //[Fact]
        //public void ReceiveMessagesWithSingleTransaction()
        //{
        //    const string queuePath = @".\Private$\MsmqSpecQueue";
        //    EnsureQueueExists(queuePath);

        //    var messageBodys = new[]
        //    {
        //        "{\"Value\":\"1\"}",
        //        "{\"Value\":\"2\"}",
        //        "{\"Value\":\"3\"}"
        //    };

        //    using (var queue = new MessageQueue(queuePath, QueueAccessMode.Send))
        //    {
        //        var count = 0;
        //        foreach (var messageBody in messageBodys)
        //        {
        //            if (count > 1) throw new Exception("Testing transaction");

        //            using (var message = new Message())
        //            {
        //                message.BodyStream = new MemoryStream(Encoding.UTF8.GetBytes(messageBody));
        //                queue.Send(message, queue.Transactional ? MessageQueueTransactionType.Single : MessageQueueTransactionType.None);
        //            }
        //            count++;
        //        }
        //    }
        //}

        // https://docs.microsoft.com/en-us/dotnet/api/system.messaging.messagequeue.beginreceive?view=netframework-4.7.2#System_Messaging_MessageQueue_BeginReceive_System_TimeSpan_System_Object_System_AsyncCallback_
        [Fact]
        public void ReceiveMessagesAsync()
        {
            const string queuePath = @".\Private$\MsmqSpecQueue";

            try
            {
                EnsureQueueExists(queuePath);

                using (var queue = new MessageQueue(queuePath, QueueAccessMode.Receive))
                {
                    if (queue.IsEmpty())
                        return;

                    // TODO: t.IsFaulted
                    var message = queue.ReceiveAsync(TimeSpan.FromMilliseconds(10))
                        .ContinueWith(t => output.WriteLine(t.Result?.Id));
                }
            }
            catch (MessageQueueException ex)
            {
                if (ex.MessageQueueErrorCode != MessageQueueErrorCode.IOTimeout)
                    throw;
            }
        }

        [Fact]
        public void MsmqShouldFailToConnectWhenWrongCredentialsAreProvided()
        {
            // TODO
        }

        [Fact]
        public void MsmqShouldSendOneMessageToAQueue()
        {
            // TODO
        }

        [Fact]
        public void MsmqShouldSendMultipleMessagesToAQueue()
        {
            // TODO
        }

        /// <summary>
        /// https://github.com/Particular/NServiceBus.Transport.Msmq/blob/0e14e4522329533c262e08e13ded07714d69cdff/src/NServiceBus.Transport.Msmq/ReceiveStrategy.cs
        /// </summary>
        [Fact]
        public void ReceiveMessagesById()
        {
            const string queuePath = @".\Private$\MsmqSpecQueue";
            EnsureQueueExists(queuePath);

            var count = 0;
            var timeout = TimeSpan.FromSeconds(30);

            using (var queue = new MessageQueue(queuePath, QueueAccessMode.Receive))
            using (var enumerator = queue.GetMessageEnumerator2())
            {
                while (enumerator.MoveNext(/*timeout*/))
                {
                    if (enumerator.Current == null)
                        continue;

                    try
                    {
                        var message = queue.ReceiveById(enumerator.Current.Id, TimeSpan.FromMilliseconds(10));
                        // message.Formatter = new XmlMessageFormatter(new[] { "System.String,mscorlib" });
                        // output.WriteLine(message?.Id);
                    }
                    catch (MessageQueueException ex)
                    {
                        if (ex.MessageQueueErrorCode != MessageQueueErrorCode.IOTimeout)
                            throw;

                        // We should only get an IOTimeout exception here if another process removed the message between us peeking and now.
                    }

                    count++;
                }

                output.WriteLine("Total messages: {0}", count);
            }
        }

        [Fact]
        public void CountMessages()
        {
            const string queuePath = @".\Private$\MsmqSpecQueue";
            EnsureQueueExists(queuePath);

            int Count(MessageQueue queue)
            {
                var count = 0;
                var enumerator = queue.GetMessageEnumerator2();
                while (enumerator.MoveNext())
                    count++;
                return count;
            }

            using (var queue = new MessageQueue(queuePath, QueueAccessMode.Receive))
            {
                var count = Count(queue);
                output.WriteLine("Total messages: {0}", count);
            }
        }

        [DebuggerStepThrough]
        private static void EnsureQueueExists(string path, bool isTransactional = false)
        {
            if (!MessageQueue.Exists(path))
            {
                MessageQueue.Create(path, isTransactional);
            }
        }
    }
}