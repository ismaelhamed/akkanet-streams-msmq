using System;
using System.Messaging;
using System.Threading.Tasks;
using Akka.Streams.Dsl;

namespace Akka.Streams.Msmq
{
    public static class MsmqFlow
    {
        public static Flow<Message, Done, NotUsed> Default(MessageQueue queue) =>
            Flow.Create<Message>()
                .SelectAsync(1 /* parallelism */, async message =>
                {
                    await queue.SendAsync(message).ConfigureAwait(false);
                    return Done.Instance;
                });
    }

    /// <summary>
    /// A container for factory methods used to build Akka.NET Streams sinks to MSMQ.
    /// Sinks can be used to send data to the Message Queuing server.
    /// </summary>
    public static class MsmqSink
    {
        /// <summary>
        /// Creates a <see cref="Sink{TIn,TMat}"/> to send messages to an MSMQ queue
        /// </summary>
        /// <param name="queue">The message queue</param>
        /// <returns>The <see cref="Sink{TIn,TMat}"/> for the MSMQ queue</returns>
        public static Sink<Message, Task> Default(MessageQueue queue) =>
            MsmqFlow.Default(queue).ToMaterialized(Sink.Ignore<Done>(), Keep.Right);
    }

    public static class MsmqSource
    {
        public static Source<Message, NotUsed> Create(IMessageQueue queue) =>
            Source.Repeat(1)
                .SelectAsync(1 /*parallelism*/, _ => queue.ReceiveAsync());

        //.TakeWhile(message => /*!settings.CloseOnEmptyReceive || */ message != null);
        //.Select(resp => resp)
        //.Buffer(100 /* maxBufferSize */, OverflowStrategy.Backpressure);
    }
}