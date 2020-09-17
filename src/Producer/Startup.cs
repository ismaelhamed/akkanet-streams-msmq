using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Messaging;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Configuration;
using Akka.IO;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Streams.Msmq;
using Akka.Streams.Msmq.Routing;
using Microsoft.Extensions.Hosting;
using Newtonsoft.Json;
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

            const int nrOfMessages = 10000;
            Log.Information("Started sending [{MessageCount}] messages... ", nrOfMessages);

            var materializer = ActorMaterializer.Create(actorSystem);
            var queuePaths = new[] {@".\Private$\MsmqPoCQueue", @".\Private$\MsmqPoCQueue2"};

            // Make sure queues exist before starting
            EnsureQueueExists(queuePaths);

            // Use cases
            PassThroughUseCase(materializer, queuePaths, nrOfMessages);
            //ByteStringUseCase(materializer, queuePaths, nrOfMessages);
            //MsmqEnvelopeUseCase(materializer, queuePaths, nrOfMessages);

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
               if (!MessageQueue.Exists(path)) MessageQueue.Create(path, transactional: true);
            }
        }

        private static void PassThroughUseCase(IMaterializer materializer, string[] queuePaths, int nrOfMessages)
        {
            // Override default broadcast routing strategy
            var settings = MessageQueueSettings.Default
                .WithRoutingStrategy(new RoundRobinRouting())
                .WithParallelism(4);

            var msmqSink = RestartSink.WithBackoff(() => MsmqSink.Create(queuePaths, settings),
                minBackoff: TimeSpan.FromSeconds(3),
                maxBackoff: TimeSpan.FromSeconds(5),
                randomFactor: 0.2 // adds 20% "noise" to vary the intervals slightly
            );

            var serializerSettings = new JsonSerializerSettings
            {
                MissingMemberHandling = MissingMemberHandling.Error,
                TypeNameHandling = TypeNameHandling.All
                //TypeNameAssemblyFormatHandling = TypeNameAssemblyFormatHandling.Full
            };

            var serializeAndProductMessage = Flow.Create<Device>()
                .Select(device =>
                {
                    // var headers = new Dictionary<string, object>
                    // {
                    //     {"ContentType", "application/json"},
                    //     {"MessageType", RemoveAssemblyInfo(typeof(Device).AssemblyQualifiedName)}
                    // };
                    // using var stream = new MemoryStream(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(headers)));

                    return new Message
                    {
                        BodyStream = new MemoryStream(ByteString.FromString(JsonConvert.SerializeObject(device, serializerSettings)).ToArray()),
                        Label = RemoveAssemblyInfo(typeof(Device).AssemblyQualifiedName),
                        //Extension = stream.ToArray(),
                        Recoverable = true
                    };
                });

            // Send messages to the queue
            Source.FromEnumerator(() => Enumerable.Range(0, nrOfMessages).GetEnumerator())
                .Select(_ => new Device {DeviceId = Guid.NewGuid(), EventTime = DateTime.Now})
                .Via(serializeAndProductMessage)
                .ToMaterialized(msmqSink, Keep.Right)
                .Run(materializer);

            // // Continuously send messages to the queue
            // Source.Tick(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1), 1)
            //     .Select(_ => new Device {DeviceId = Guid.NewGuid(), EventTime = DateTime.Now})
            //     .Via(serializeAndProductMessage)
            //     //.Log("Tick", message => message.Id)
            //     .RunWith(msmqSink, materializer);
        }

        // private static void ByteStringUseCase(IMaterializer materializer, string[] queuePaths, int nrOfMessages)
        // {
        //     var msmqSink = RestartSink.WithBackoff(() => MsmqSink.FromByteString(queuePaths),
        //         minBackoff: TimeSpan.FromSeconds(3),
        //         maxBackoff: TimeSpan.FromSeconds(5),
        //         randomFactor: 0.2 // adds 20% "noise" to vary the intervals slightly
        //     );
        //
        //     var serializeDevice = Flow.Create<Device>()
        //         .Select(device =>
        //         {
        //             var json = JsonConvert.SerializeObject(device);
        //             return ByteString.FromString(json);
        //         });
        //
        //     // Send messages to the queue
        //     Source.FromEnumerator(() => Enumerable.Range(0, nrOfMessages).GetEnumerator())
        //         .Select(_ => new Device {DeviceId = Guid.NewGuid(), EventTime = DateTime.Now})
        //         .Via(serializeDevice)
        //         .RunWith(msmqSink, materializer);
        // }

        //private static void MsmqEnvelopeUseCase(IMaterializer materializer, string[] queuePaths, int nrOfMessages)
        //{
        //    var msmqSink = RestartSink.WithBackoff(() => MsmqSink.FromMessage(queuePaths),
        //        minBackoff: TimeSpan.FromSeconds(3),
        //        maxBackoff: TimeSpan.FromSeconds(5),
        //        randomFactor: 0.2 // adds 20% "noise" to vary the intervals slightly
        //    );

        //    var produceEnvelop = Flow.Create<Device>()
        //        .Select(device =>
        //        {
        //            var headers = new Dictionary<string, object>
        //            {
        //                {"ContentType", "application/json"},
        //                {"MessageType", RemoveAssemblyInfo(typeof(Device).AssemblyQualifiedName)}
        //            };

        //            return new MessageEnvelope(
        //                ByteString.FromString(JsonConvert.SerializeObject(device)),
        //                headers: headers);
        //        });

        //    // Send messages to the queue
        //    Source.FromEnumerator(() => Enumerable.Range(0, nrOfMessages).GetEnumerator())
        //        .Select(_ => new Device {DeviceId = Guid.NewGuid(), EventTime = DateTime.Now})
        //        .Via(produceEnvelop)
        //        .RunWith(msmqSink, materializer);
        //}

        /// <summary>
        ///   Removes all assembly info from the specified type name.
        /// </summary>
        /// <param name="typeName">Type name to remove assembly info from.</param>
        /// <returns>Type name without assembly info.</returns>
        public static string RemoveAssemblyInfo(string typeName)
        {
            // Get start of "Version=..., Culture=..., PublicKeyToken=..." string.
            var versionIndex = typeName.IndexOf("Version=", StringComparison.Ordinal);
            if (versionIndex >= 0)
            {
                // Get end of "Version=..., Culture=..., PublicKeyToken=..." string for generics.
                var endIndex = typeName.IndexOf(']', versionIndex);

                // Get end of "Version=..., Culture=..., PublicKeyToken=..." string for non-generics.
                endIndex = endIndex >= 0 ? endIndex : typeName.Length;

                // Remove version info.
                typeName = typeName.Remove(versionIndex - 2, endIndex - versionIndex + 2);
            }

            return typeName;
        }
    }
}