// Copyright (c) 2023 Ismael Hamed. All rights reserved.
// See LICENSE file in the root folder for full license information.

using System;
using System.Messaging;

namespace Akka.Streams.Msmq.Tests
{
    public class MessageQueueFixture : IDisposable
    {
        public string SourceQueuePath { get; } = @".\Private$\SourceQueue";

        public string DestinationQueuePath { get; } = @".\Private$\DestinationQueue";

        public IMessageFormatter Formatter { get; } = new XmlMessageFormatter(new[] { typeof(string) });

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                // clean up code
                if (MessageQueue.Exists(SourceQueuePath)) MessageQueue.Delete(SourceQueuePath);
                if (MessageQueue.Exists(DestinationQueuePath)) MessageQueue.Delete(DestinationQueuePath);
            }
        }
    }
}