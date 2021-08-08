using System.Messaging;
using Akka.Streams.Dsl;
using Akka.Util;

namespace Akka.Streams.Msmq
{
    public static class MsmqSource
    {
        //private static bool closeOnEmptyReceive = false;

        public static Source<Option<Message>, NotUsed> Create(MessageQueue queue) =>
            Source.Repeat(1)
                .SelectAsync(1 /*parallelism*/, _ => queue.ReceiveAsync())
                .Where(option => option.HasValue);

        //.TakeWhile(msg => !closeOnEmptyReceive || msg.HasValue);
    }
}