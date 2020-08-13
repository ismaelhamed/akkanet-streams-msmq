using System;
using System.IO;
using System.Messaging;
using System.Threading;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Cluster;
using Akka.Configuration;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Streams.Msmq;
using Microsoft.Extensions.Hosting;
using Serilog;

namespace Consumer
{
    public class Startup : IHostedService
    {
        private ActorSystem actorSystem;
        //private readonly Random random = new Random();

        public Task StartAsync(CancellationToken cancellationToken)
        {
            Log.Information("Starting actor system...");

            var hoconConfig = ConfigurationFactory.ParseString(File.ReadAllText(AppDomain.CurrentDomain.BaseDirectory + "application.conf"));
            actorSystem = ActorSystem.Create("MsmqPoC", hoconConfig);

            Cluster.Get(actorSystem).RegisterOnMemberUp(() =>
            {
                Log.Information("Started receiving messages... ");

                var materializer = ActorMaterializer.Create(actorSystem);
                var restartSource = RestartSource.WithBackoff(() =>
                    {
                        // if the queue becomes stale, we need to recreate the queue
                        var queue = CreateQueue(@".\Private$\MsmqPoCQueue");
                        return MsmqSource.Create(queue);
                    },
                    minBackoff: TimeSpan.FromSeconds(3),
                    maxBackoff: TimeSpan.FromSeconds(30),
                    randomFactor: 0.2 // adds 20% "noise" to vary the intervals slightly
                );

                restartSource
                    .RunForeach(m => { }, materializer);
                    //.RunForeach(m => Log.Information("Received msg [{Message}]", (string) m.Value?.Body), materializer);

                // MsmqSource.Create(queue)
                //     //.Log("Received")
                //     //.Throttle(1, TimeSpan.FromSeconds(5), 1, ThrottleMode.Shaping)
                //     // .Select(m =>
                //     // {
                //     //     if (m.HasValue) count++;
                //     //     //if (count >= 50000) Log.Information("Received [{MessageCount}] messages so far... ", count);
                //     //     return m.Value;
                //     // })
                //     // .RunWith(Sink.Ignore<Message>(), materializer);
                //     .Select(m => m.Value)
                //     .Recover(exception =>
                //     {
                //         Log.Error("Stream threw an exception: {Reason}", exception.Message);
                //         return Option<Message>.None;
                //     })
                //     .RunForeach(m => Log.Information("Received msg [{Message}]", (string) m?.Body), materializer);

                // actorSystem.Scheduler.Advanced.ScheduleRepeatedly(TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(5), () =>
                // {
                //     Log.Information("Received [{MessageCount}] messages so far... ", count);
                //
                //     // Send messages to the queue
                //     Enumerable.Range(0, random.Next(1, 100)).ForEach(async i =>
                //     {
                //         await queue.SendAsync(new Message(DateTime.Now.ToLongTimeString()) {Recoverable = true});
                //     });
                //
                //     Source.FromEnumerator(() => Enumerable.Range(0, random.Next(1, 100)).GetEnumerator())
                //         .Select(_ => new Message(DateTime.Now.ToLongTimeString()) {Recoverable = true})
                //         .ToMaterialized(MsmqSink.Default(queue), Keep.Both)
                //         .Run(materializer);
                // });
            });
            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            Log.Information("Stopping actor system...");
            return CoordinatedShutdown.Get(actorSystem).Run(CoordinatedShutdown.ClrExitReason.Instance);
        }

        static MessageQueue CreateQueue(string queuePath) =>
            new MessageQueue(queuePath)
            {
                Formatter = new XmlMessageFormatter(new[] {typeof(string)})
            };
    }
}