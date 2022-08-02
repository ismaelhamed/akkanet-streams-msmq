using System;
using System.Messaging;
using System.Threading.Tasks;
using Akka.Streams.Dsl;
using Akka.Util;

namespace Akka.Streams.Msmq
{
    public static class MsmqSource
    {
        //public static Source<Message, NotUsed> Create(string queuePath, MessageQueueSettings queueSettings = null)
        //{
        //    var settings = queueSettings ?? MessageQueueSettings.Default;

        //    return Source.UnfoldResource<Option<Message>, MessageQueue>(
        //            () => CreateMessageQueue(queuePath, settings),
        //            queue => queue.TryReceive(),
        //            queue => queue.Close())
        //        .Where(option => option.HasValue)
        //        .Select(option => option.Value);
        //}

        public static Source<Message, NotUsed> Create(string queuePath, MessageQueueSettings queueSettings = null)
        {
            var settings = queueSettings ?? MessageQueueSettings.Default;
            var queue = CreateMessageQueue(queuePath, settings);

            return Source.Repeat(1)
                .SelectAsync(Math.Max(2, Environment.ProcessorCount), _ => queue.ReceiveAsync())
                .Collect(option => option.HasValue, option => option.Value);
        }

        private static MessageQueue CreateMessageQueue(string path, MessageQueueSettings settings) =>
            new MessageQueue(path, settings.DenySharedReceive, settings.EnableConnectionCache, settings.AccessMode)
            {
                UseJournalQueue = settings.UseJournalQueue,
                Formatter = settings.MessageFormatter,
                MessageReadPropertyFilter = settings.MessagePropertyFilter
            };
    }
}