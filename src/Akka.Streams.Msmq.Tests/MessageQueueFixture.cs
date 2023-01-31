// Copyright (c) 2023 Ismael Hamed. All rights reserved.
// See LICENSE file in the root folder for full license information.

using System.Messaging;

namespace Akka.Streams.Msmq.Tests
{
    public class MessageQueueFixture
    {
        public string QueuePath { get; } = @".\Private$\MsmqSpecQueue";

        public IMessageFormatter Formatter { get; } = new XmlMessageFormatter(new[] { typeof(string) });
    }
}