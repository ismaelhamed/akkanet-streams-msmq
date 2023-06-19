using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Messaging;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Serialization;
using Akka;
using Akka.Actor;
using Akka.Configuration;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Streams.Msmq;
using Akka.Util.Internal;
using Microsoft.Extensions.Hosting;
using Serilog;
using TitaniumSTG.Messages.HE.DlmsHE;
using Decider = Akka.Streams.Supervision.Decider;
using Directive = Akka.Streams.Supervision.Directive;

namespace Producer
{
    public class Startup : IHostedService
    {
        private Config config;
        private ActorSystem actorSystem;

        private static readonly XmlSerializer headerSerializer = new XmlSerializer(typeof(List<HeaderInfo>));

        public static readonly Decider CustomDecider = cause => cause is MessageQueueException
            ? Directive.Restart
            : Directive.Stop;

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            Log.Information("Starting actor system...");

            config = ConfigurationFactory.ParseString(File.ReadAllText(AppDomain.CurrentDomain.BaseDirectory + "application.conf"));
            actorSystem = ActorSystem.Create("MsmqPoC", config);

            const int nrOfMessages = 100_000;
            Log.Information("Started sending [{MessageCount}] messages... ", nrOfMessages);

            var materializer = ActorMaterializer.Create(actorSystem);
            var producerConfig = config.GetConfig("alpakka.msmq.producer");
            var queuePaths = (await DiscoverySupport.ReadAddresses(producerConfig, actorSystem)).ToArray();

            // Make sure queues exist before starting
            // EnsureQueueExists(queuePaths);

            // Use cases
            //ActorPublisher(materializer, queuePaths, nrOfMessages);
            //PassThroughUseCase(materializer, queuePaths, nrOfMessages);
            //ByteStringUseCase(materializer, queuePaths, nrOfMessages);
            //MsmqEnvelopeUseCase(materializer, queuePaths, nrOfMessages);

            //PartitionHubScenario(materializer);
            RoundRobinScenario(materializer, nrOfMessages);
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
                if (!MessageQueue.Exists(path)) MessageQueue.Create(path, true);
            }
        }

        #region Scenario 1

        private void PassThroughUseCase(IMaterializer materializer, string[] queuePaths, int nrOfMessages)
        {
            var settings = MessageQueueSettings.Default;

            var msmqSink = RestartSink.WithBackoff(() => MsmqSink.Default(settings, queuePaths.First()),
                minBackoff: TimeSpan.FromSeconds(3),
                maxBackoff: TimeSpan.FromSeconds(30),
                randomFactor: 0.2 // adds 20% "noise" to vary the intervals slightly
            );

            //var serializerSettings = new JsonSerializerSettings
            //{
            //    MissingMemberHandling = MissingMemberHandling.Error,
            //    //TypeNameHandling = TypeNameHandling.All,
            //    //TypeNameAssemblyFormatHandling = TypeNameAssemblyFormatHandling.Full
            //};

            var serializeAndProductMessage = Flow.Create<NotificationResult>()
                .Select(device =>
                {
                    var headers = new List<HeaderInfo>
                    {
                        new HeaderInfo {Key = "NServiceBus.ContentType", Value = "text/xml"},
                        new HeaderInfo {Key = "NServiceBus.EnclosedMessageTypes", Value = typeof(NotificationResult).FullName}
                        //new HeaderInfo { Key = "NServiceBus.EnclosedMessageTypes", Value = RemoveAssemblyInfo(typeof(CommandNotificationResult).AssemblyQualifiedName) }
                    };

                    // XML
                    using var stream = new MemoryStream();
                    headerSerializer.Serialize(stream, headers);

                    //var headers = new Dictionary<string, string>
                    //{
                    //    { "ContentType", "application/json" },
                    //    { "MessageType", RemoveAssemblyInfo(typeof(Device).AssemblyQualifiedName) }
                    //};

                    // // JSON
                    // using var stream = new MemoryStream(ByteString.FromString(JsonConvert.SerializeObject(headers)).ToArray());

                    return new Message(device)
                    {
                        Recoverable = false,
                        Label = typeof(NotificationResult).FullName ?? string.Empty,
                        Extension = stream.ToArray()
                    };

                    //return new Message
                    //{
                    //    Recoverable = true,
                    //    BodyStream = new MemoryStream(ByteString.FromString(JsonConvert.SerializeObject(device, serializerSettings)).ToArray()),
                    //    Label = typeof(CommandNotificationResult).FullName ?? string.Empty,
                    //    Extension = stream.ToArray()
                    //};
                });

            var index = 12999;

            // Send messages to the queue
            Source.FromEnumerator(() => Enumerable.Range(0, nrOfMessages).GetEnumerator())
                //.Select(_ => new Device { DeviceId = Guid.NewGuid(), EventTime = DateTime.Now })
                .Select(_ =>
                {
                    index++;
                    var notification = new Notification
                    {
                        CorrelationId = index.ToString(),
                        DeviceID = Guid.NewGuid().ToString("N"),
                        DlmsSpecification = "",
                        IsSuccessful = true,
                        FromDatetime = DateTime.Today.AddDays(-1),
                        ToDatetime = DateTime.Today
                    };
                    return new NotificationResult { CorrelationId = notification.CorrelationId, Notification = "Hello!" /* notification.Serialize()*/ };
                })
                .Via(serializeAndProductMessage)
                .ToMaterialized(msmqSink, Keep.Right)
                .Run(materializer);

            // // Continuously send messages to the queue
            // Source.Tick(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1), 1)
            //     //.Select(_ => new Device { DeviceId = Guid.NewGuid(), EventTime = DateTime.Now })
            //     .Select(_ => new CommandNotificationResult { NotificationId = Guid.NewGuid() })
            //     .Via(serializeAndProductMessage)
            //     .Log("device", message => message.Id)
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
        //            var headers = new Dictionary<string, string>
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
        /// Removes all assembly info from the specified type name.
        /// </summary>
        /// <param name="typeName">Type name to remove assembly info from.</param>
        /// <returns>Type name without assembly info.</returns>
        public static string RemoveAssemblyInfo(string typeName)
        {
            // Get start of "Version=..., Culture=..., PublicKeyToken=..." string.
            var versionIndex = typeName.IndexOf("Version=", StringComparison.Ordinal);
            if (versionIndex < 0) return typeName;

            // Get end of "Version=..., Culture=..., PublicKeyToken=..." string for generics.
            var endIndex = typeName.IndexOf(']', versionIndex);

            // Get end of "Version=..., Culture=..., PublicKeyToken=..." string for non-generics.
            endIndex = endIndex >= 0 ? endIndex : typeName.Length;

            // Remove version info.
            typeName = typeName.Remove(versionIndex - 2, endIndex - versionIndex + 2);

            return typeName;
        }

        #endregion

        #region Scenario2

        private void PartitionHubScenario(IMaterializer materializer)
        {
            // New instance of the partitioner function and its state is created
            // for each materialization of the PartitionHub.
            static Func<PartitionHub.IConsumerInfo, string, long> RoundRobin()
            {
                var i = -1L;
                return (info, element) =>
                {
                    i++;
                    return info.ConsumerByIndex((int)(i % info.Size));
                };
            }

            var workers = new[] { "Consumer1", "Consumer2", "Consumer3" };

            // A simple producer that publishes a new "message-" every second
            var producer = Source.Tick(TimeSpan.FromSeconds(1), TimeSpan.FromMilliseconds(300), "message")
                .MapMaterializedValue(_ => NotUsed.Instance)
                .ZipWith(Source.From(Enumerable.Range(1, 100)), (msg, i) => $"{msg}-{i}");

            // Attach a PartitionHub Sink to the producer. This will materialize to a
            // corresponding Source.
            // (We need to use toMat and Keep.right since by default the materialized
            // value to the left is used)
            var runnableGraph = producer.ToMaterialized(
                PartitionHub.StatefulSink(RoundRobin, startAfterNrOfConsumers: 1, bufferSize: workers.Length),
                Keep.Right);

            // By running/materializing the producer, we get back a Source, which
            // gives us access to the elements published by the producer.
            var fromProducer = runnableGraph.Run(materializer);

            //// Print out messages from the producer in two independent consumers
            //fromProducer.RunForeach(msg => Console.WriteLine("Consumer1: " + msg), materializer);
            //fromProducer.RunForeach(msg => Console.WriteLine("Consumer2: " + msg), materializer);
            //fromProducer.To(Sink.ForEach<string>(msg => Console.WriteLine("Consumer3: " + msg))).Run(materializer);

            foreach (var worker in workers)
            {
                // Print out messages from the producer in independent consumers
                fromProducer.To(Sink.ForEach<string>(msg => Console.WriteLine($"{worker}: {msg}"))).Run(materializer);
            }
        }

        private void RoundRobinScenario(IMaterializer materializer, int nrOfMessages)
        {
            var indexes = new ConcurrentDictionary<string, int>();

            Func<PartitionHub.IConsumerInfo, Message, long> RoundRobin()
            {
                // Start with -1 so the current index will be at 0 after the first increment.
                var index = -1L;
                return (info, _) =>
                {
                    index++;
                    return info.ConsumerByIndex((int)(index % info.Size));
                };

                //return (info, message) =>
                //{
                //    // Start with -1 so the current index will be at 0 after the first increment.
                //    var current = indexes.GetOrAdd(message.Label, _ => -1);

                //    var next = indexes[message.Label] = Interlocked.Increment(ref current);
                //    var index = (next & int.MaxValue) % info.Size;
                //    return info.ConsumerIds[index < 0 ? info.Size + index - 1 : index];
                //};
            }

            var queuePaths = new[]
            {
                ".\\Private$\\DynamicRoutingTest",
                ".\\Private$\\DynamicRoutingTest2"
            };

            static string ToXmlString<T>(T value)
            {
                var settings = new XmlWriterSettings
                {
                    Encoding = new UnicodeEncoding(false, false),
                    Indent = false,
                    OmitXmlDeclaration = false,
                    CheckCharacters = false
                };

                using var stream = new StringWriter();
                using var writer = XmlWriter.Create(stream, settings);

                var serializer = new XmlSerializer(value.GetType());
                var namespaces = new XmlSerializerNamespaces(new[] { XmlQualifiedName.Empty });

                serializer.Serialize(writer, value, namespaces);
                return stream.ToString();
            }

            static NotificationResult ProduceNotificationResult(int i)
            {
                var notification = new Notification
                {
                    CorrelationId = i.ToString(),
                    DeviceID = Guid.NewGuid().ToString("N"),
                    DlmsSpecification = "",
                    IsSuccessful = true,
                    FromDatetime = DateTime.Today.AddDays(-1),
                    ToDatetime = DateTime.Today
                };

                return new NotificationResult { CorrelationId = notification.CorrelationId, Notification = ToXmlString(notification) };
            }

            var serialiazeMessage = Flow.Create<NotificationResult>()
                .Select(result =>
                {
                    //return new Message(result)
                    //{
                    //    // MSMQ requires the id's to be in the {guid}\{incrementing number} format
                    //    CorrelationId = $"{Guid.NewGuid()}\\0",
                    //    Label = typeof(NotificationResult).FullName ?? string.Empty
                    //};

                    //var settings = new JsonSerializerSettings
                    //{
                    //    MissingMemberHandling = MissingMemberHandling.Error,
                    //    TypeNameHandling = TypeNameHandling.None // because CA2326
                    //};

                    //return new Message
                    //{
                    //    // MSMQ requires the id's to be in the {guid}\{incrementing number} format
                    //    CorrelationId = $"{Guid.NewGuid()}\\0",
                    //    Label = typeof(NotificationResult).FullName ?? string.Empty,
                    //    BodyStream = new MemoryStream(ByteString.FromString(JsonConvert.SerializeObject(result, settings)).ToArray())
                    //};

                    return new Message
                    {
                        // MSMQ requires the id's to be in the {guid}\{incrementing number} format
                        CorrelationId = $"{Guid.NewGuid()}\\0",
                        Label = typeof(NotificationResult).FullName ?? string.Empty,
                        BodyStream = new MemoryStream(JsonSerializer.SerializeToUtf8Bytes(result))
                    };
                });

            var latch = new CountdownEvent(nrOfMessages);
            var sw = Stopwatch.StartNew();

            // Send messages to the queue
            var source = Source.From(Enumerable.Range(0, nrOfMessages))
                .Select(ProduceNotificationResult)
                .Via(serialiazeMessage)
                .WireTap(_ => latch.Signal())
                .ToMaterialized(PartitionHub.StatefulSink(RoundRobin, 1, queuePaths.Length), Keep.Right)
                .Run(materializer);

            var sinkSettings = MessageQueueSettings.Default.WithParallelism(2);
            var restartSettings = RestartSettings.Create(TimeSpan.FromSeconds(3), TimeSpan.FromSeconds(30), 0.2);

            // Add dynamic sinks
            queuePaths.ForEach(path => source.RunWith(RestartSink.WithBackoff(() => MsmqSink.Default(sinkSettings, path), restartSettings), materializer));

            latch.Wait();
            sw.Stop();
            Console.WriteLine("Sending [{0}] messages took {1} sec.", nrOfMessages, sw.Elapsed.TotalSeconds);
        }

        private void ActorPublisher(IMaterializer materializer, string[] queuePaths, int nrOfMessages)
        {
            var settings = MessageQueueSettings.Default;

            var restartSettings = RestartSettings.Create(TimeSpan.FromSeconds(3), TimeSpan.FromSeconds(30), 0.2);
            var msmqSink = RestartSink.WithBackoff(() => MsmqSink.Default(settings, queuePaths.First()), restartSettings);

            static string ToXmlString<T>(T value)
            {
                var settings = new XmlWriterSettings
                {
                    Encoding = new UnicodeEncoding(false, false),
                    Indent = false,
                    OmitXmlDeclaration = false,
                    CheckCharacters = false
                };

                using var stream = new StringWriter();
                using var writer = XmlWriter.Create(stream, settings);

                var serializer = new XmlSerializer(value.GetType());
                var namespaces = new XmlSerializerNamespaces(new[] { XmlQualifiedName.Empty });

                serializer.Serialize(writer, value, namespaces);
                return stream.ToString();
            }

            var produceMessage = Flow.Create<NotificationResult>()
                .Select(result =>
                {
                    var headers = new List<HeaderInfo>
                    {
                        new HeaderInfo {Key = "NServiceBus.ContentType", Value = "text/xml"},
                        new HeaderInfo {Key = "NServiceBus.EnclosedMessageTypes", Value = typeof(NotificationResult).FullName}
                    };

                    // XML
                    using var stream = new MemoryStream();
                    headerSerializer.Serialize(stream, headers);

                    return new Message(result)
                    {
                        Recoverable = false,
                        Label = typeof(NotificationResult).FullName ?? string.Empty,
                        Extension = stream.ToArray()
                    };
                });

            //// Consumer
            //Source.ActorPublisher<NotificationResult>(Props.Create<NotificationPublisher>())
            //    .Via(produceMessage)
            //    .ToMaterialized(msmqSink, Keep.Right)
            //    .Run(materializer);

            //// Producer
            //Source.From(Enumerable.Range(1, nrOfMessages))
            //    .Select(i =>
            //    {
            //        var notification = new Notification
            //        {
            //            CorrelationId = i.ToString(),
            //            DeviceID = Guid.NewGuid().ToString("N"),
            //            DlmsSpecification = "",
            //            IsSuccessful = true,
            //            FromDatetime = DateTime.Today.AddDays(-1),
            //            ToDatetime = DateTime.Today
            //        };
            //        return new NotificationResult { CorrelationId = notification.CorrelationId, Notification = ToXmlString(notification) };
            //    })
            //    .RunWith(Sink.ForEach<NotificationResult>(result => actorSystem.EventStream.Publish(result)), materializer);

            // Send messages to the queue
            Source.FromEnumerator(() => Enumerable.Range(0, nrOfMessages).GetEnumerator())
                .Select(i =>
                {
                    var notification = new Notification
                    {
                        CorrelationId = i.ToString(),
                        DeviceID = Guid.NewGuid().ToString("N"),
                        DlmsSpecification = "",
                        IsSuccessful = true,
                        FromDatetime = DateTime.Today.AddDays(-1),
                        ToDatetime = DateTime.Today
                    };
                    return new NotificationResult { CorrelationId = notification.CorrelationId, Notification = ToXmlString(notification) };
                })
                .Via(produceMessage)
                //.WireTap(msg => Log.Information("message passed"))
                .ToMaterialized(msmqSink, Keep.Right)
                .Run(materializer);
        }

        #endregion

        static readonly Random Random = new Random();

        static string GenerateId(int id = 0)
        {
            if (id == 0) id = Random.Next(1, 10000);
            return $"{Guid.NewGuid()}\\{id}";
        }
    }
}