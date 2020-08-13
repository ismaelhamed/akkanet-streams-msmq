//-----------------------------------------------------------------------
// <copyright file="MsmqFlow.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2020 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2020 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Messaging;
using Akka.Streams.Dsl;

namespace Akka.Streams.Msmq
{
    public static class MsmqFlow
    {
        private static MessageQueue queue;
        private static readonly Func<string, MessageQueue> messageQueueFactory = queuePath =>
            new MessageQueue(queuePath)
            {
                Formatter = new XmlMessageFormatter(new[] {typeof(string)})
            };

        public static Flow<Message, Done, NotUsed> Default(MessageQueue queue1) =>
            Flow.Create<Message>()
                .SelectAsync(1 /* parallelism */, async message =>
                {
                    await queue1.SendAsync(message).ConfigureAwait(false);
                    return Done.Instance;
                });

        public static Flow<Message, Done, NotUsed> Default(string queuePath)
        {
            queue ??= messageQueueFactory(queuePath);

            return Flow.Create<Message>()
                .SelectAsync(1 /* parallelism */, async message =>
                {
                    await queue.SendAsync(message).ConfigureAwait(false);
                    return Done.Instance;
                });
        }
    }
}