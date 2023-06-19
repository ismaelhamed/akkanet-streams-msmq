using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using Akka.Event;
using Akka.Logger.Serilog;
using Akka.Streams.Actors;
using TitaniumSTG.Messages.HE.DlmsHE;

namespace Producer.Actors
{
    public class NotificationPublisher : ActorPublisher<NotificationResult>
    {
        private readonly EventStream eventStream = Context.System.EventStream;
        private readonly Queue<NotificationResult> buffer = new Queue<NotificationResult>();

        protected ILoggingAdapter Log { get; } = Context.GetLogger<SerilogLoggingAdapter>();

        protected override void PreStart()
        {
            base.PreStart();
            eventStream.Subscribe<NotificationResult>(Self);
        }

        protected override void PostStop()
        {
            eventStream.Unsubscribe(Self);
            base.PostStop();
        }

        protected override bool Receive(object message)
        {
            switch (message)
            {
                case NotificationResult notification:
                    buffer.Enqueue(notification);
                    PublishIfNeeded();
                    return true;
                case Request request:
                    PublishIfNeeded();
                    return true;
                case Cancel _:
                    Log.Warning("Stopping");
                    Context.Stop(Self);
                    return true;
            }

            return false;
        }

        private void PublishIfNeeded()
        {
            while (buffer.Any() && IsActive && TotalDemand > 0)
                OnNext(buffer.Dequeue());
        }
    }
}