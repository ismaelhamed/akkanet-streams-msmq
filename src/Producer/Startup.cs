using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Messaging;
using System.Threading;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Configuration;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Streams.Msmq;
using Akka.Util.Internal;
using Microsoft.Extensions.Hosting;
using Serilog;
using Decider = Akka.Streams.Supervision.Decider;
using Directive = Akka.Streams.Supervision.Directive;

namespace Producer
{
    public class Startup : IHostedService
    {
        private ActorSystem actorSystem;

        public static readonly Decider CustomDecider = cause => cause is MessageQueueException
            ? Directive.Restart
            : Directive.Stop;

        public Task StartAsync(CancellationToken cancellationToken)
        {
            Log.Information("Starting actor system...");

            var hoconConfig = ConfigurationFactory.ParseString(File.ReadAllText(AppDomain.CurrentDomain.BaseDirectory + "application.conf"));
            actorSystem = ActorSystem.Create("MsmqPoC", hoconConfig);

            const int nrOfMessages = 5;
            Log.Information("Started sending [{MessageCount}] messages... ", nrOfMessages);

            var materializer = ActorMaterializer.Create(actorSystem);
            var queuePaths = new[] {@".\Private$\MsmqPoCQueue", @".\Private$\MsmqPoCQueue2", @".\Private$\MsmqPoCQueue3"};

            // Make sure queues exist before starting
            EnsureQueueExists(queuePaths);

            var msmqSink = RestartSink.WithBackoff(() => MsmqSink.Default(queuePaths),
                minBackoff: TimeSpan.FromSeconds(3),
                maxBackoff: TimeSpan.FromSeconds(5),
                randomFactor: 0.2 // adds 20% "noise" to vary the intervals slightly
            );

            // // Send messages to the queue
            // Source.FromEnumerator(() => Enumerable.Range(0, nrOfMessages).GetEnumerator())
            //     .Select(_ => new Message(DateTime.Now.ToLongTimeString()) {Recoverable = true, TimeToBeReceived = TimeSpan.FromHours(1)})
            //     .ToMaterialized(msmqSink, Keep.Both)
            //     .Run(materializer);

            // Continuously send messages to the queue
            Source.Tick(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1), 1)
                .Select(_ => new Message(DateTime.Now.ToLongTimeString()) {Recoverable = true})
                .Log("Tick", message => message.Body.ToString())
                .RunWith(msmqSink, materializer);

            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            Log.Information("Stopping actor system...");
            return CoordinatedShutdown.Get(actorSystem).Run(CoordinatedShutdown.ClrExitReason.Instance);
        }

        private static void EnsureQueueExists(IEnumerable<string> queuePaths)
        {
            foreach (var path in queuePaths)
            {
                try
                {
                    if (!MessageQueue.Exists(path)) MessageQueue.Create(path, transactional: true);
                }
                catch { }
            }
        }

        private static Task SendMessages(int nrOfMessages)
        {
            Log.Information("Started sending [{MessageCount}] messages... ", nrOfMessages);

            var queue = new MessageQueue(@".\Private$\MsmqSpecQueue")
            {
                Formatter = new XmlMessageFormatter(new[] {typeof(string)})
            };

            Enumerable.Range(0, nrOfMessages).ForEach(async i => await queue.SendAsync(new Message(DateTime.Now.ToLongTimeString())));
            return Task.CompletedTask;
        }
    }
}