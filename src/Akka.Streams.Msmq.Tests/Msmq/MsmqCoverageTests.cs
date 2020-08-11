using System;
using System.Diagnostics;
using System.IO;
using System.Messaging;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
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

            var messageBody = new SomeMessage {SomeProperty = 1234};

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
        public void SendMessagesWithPerMessageTransaction()
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

        [Fact]
        public void ReceiveMessages()
        {
            const string queuePath = @".\Private$\MsmqSpecQueue";
            EnsureQueueExists(queuePath);

            using (var queue = new MessageQueue(queuePath, QueueAccessMode.SendAndReceive))
            {
                // Send a message to the queue.
                queue.Send("Example Message");

                // Add an event handler for the ReceiveCompleted event.
                queue.ReceiveCompleted += (sender, args) =>
                {
                    // Connect to the queue.
                    var mq = (MessageQueue) sender;

                    try
                    {
                        // End the asynchronous receive operation.
                        var message = mq.EndReceive(args.AsyncResult);

                        // Do something with the message
                        output.WriteLine("Received message with id [{0}]", message.Id);
                    }
                    catch (MessageQueueException ex) when (ex.MessageQueueErrorCode == MessageQueueErrorCode.IOTimeout)
                    {
                        // Ignore timeout
                    }

                    // Restart the asynchronous receive operation.
                    mq.BeginReceive(TimeSpan.FromMilliseconds(10));
                };

                // Begin the asynchronous receive operation.
                queue.BeginReceive(TimeSpan.FromMilliseconds(10));

                // Simulate doing other work on the current thread.
                Thread.Sleep(TimeSpan.FromSeconds(3));
            }
        }

        [Theory]
        [InlineData(10)]
        public async Task ReceiveMessageAsync(double milliseconds)
        {
            const string queuePath = @".\Private$\MsmqSpecQueue";
            EnsureQueueExists(queuePath);

            using (var queue = new MessageQueue(queuePath, QueueAccessMode.SendAndReceive))
            {
                try
                {
                    // Send a message to the queue.
                    queue.Send("Example Message");

                    // Begin the asynchronous receive operation.
                    var message = await queue.ReceiveAsync(TimeSpan.FromMilliseconds(milliseconds))
                        .ConfigureAwait(false);

                    if (message.HasValue)
                    {
                        // Do something with the message
                        output.WriteLine("Received message with id [{0}]", message.Value.Id);
                    }
                }
                catch (MessageQueueException ex) when (ex.MessageQueueErrorCode == MessageQueueErrorCode.IOTimeout)
                {
                    // Ignore timeout
                }
            }
        }

        [Theory]
        [InlineData(10)]
        public async Task ReceiveMessagesAsync(double milliseconds)
        {
            const string queuePath = @".\Private$\MsmqSpecQueue";
            EnsureQueueExists(queuePath);

            // Define the cancellation token.
            var source = new CancellationTokenSource();
            source.CancelAfter(TimeSpan.FromSeconds(3));
            var cancellationToken = source.Token; // Previously provided token

            using (var queue = new MessageQueue(queuePath, QueueAccessMode.SendAndReceive))
            {
                // Send a few messages to the queue.
                queue.Send("Example Message");
                queue.Send("Example Message");
                queue.Send("Example Message");
                queue.Send("Example Message");
                queue.Send("Example Message");

                while (!cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        // Begin the asynchronous receive operation.
                        var message = await queue.ReceiveAsync(TimeSpan.FromMilliseconds(milliseconds))
                            .ConfigureAwait(false);

                        if (message.HasValue)
                        {
                            // Do something with the message
                            output.WriteLine("Received message with id [{0}]", message.Value.Id);
                        }
                    }
                    catch (MessageQueueException ex) when (ex.MessageQueueErrorCode == MessageQueueErrorCode.IOTimeout)
                    {
                        // Ignore timeout
                    }
                }
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
                while (enumerator.MoveNext( /*timeout*/))
                {
                    if (enumerator.Current == null)
                        continue;

                    try
                    {
                        var message = queue.ReceiveById(enumerator.Current.Id, TimeSpan.FromMilliseconds(10));
                        // message.Formatter = new XmlMessageFormatter(new[] { "System.String,mscorlib" });

                        output.WriteLine(message?.Id);
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