using System;
using System.Collections.Concurrent;
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
    public class RoundRobinScenario : IHostedService
    {
        private Config config;
        private ActorSystem actorSystem;

        public static readonly Decider CustomDecider = cause => cause is MessageQueueException
            ? Directive.Restart
            : Directive.Stop;

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            Log.Information("Starting actor system...");

            config = ConfigurationFactory.ParseString(File.ReadAllText(AppDomain.CurrentDomain.BaseDirectory + "application.conf"));
            actorSystem = ActorSystem.Create("MsmqPoC", config);

            const int nrOfMessages = 1_000;
            Log.Information("Started sending [{MessageCount}] messages... ", nrOfMessages);

            var materializer = ActorMaterializer.Create(actorSystem);
            var producerConfig = config.GetConfig("alpakka.msmq.target");
            var queuePaths = (await DiscoverySupport.ReadAddresses(producerConfig, actorSystem)).ToArray();

            RunScenario(materializer, queuePaths, nrOfMessages);
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            Log.Information("Stopping actor system...");
            return CoordinatedShutdown.Get(actorSystem).Run(CoordinatedShutdown.ClrExitReason.Instance);
        }

        private void RunScenario(IMaterializer materializer, string[] queuePaths, int nrOfMessages)
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

            var serialiazeFlow = Flow.Create<NotificationResult>()
                .Select(result =>
                {
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
                .Via(serialiazeFlow)
                .WireTap(_ => latch.Signal())
                .ToMaterialized(PartitionHub.StatefulSink(RoundRobin, 1, queuePaths.Length), Keep.Right)
                .Run(materializer);

            var sinkSettings = MessageQueueSettings.Default.WithParallelism(2);
            var restartSettings = RestartSettings.Create(TimeSpan.FromSeconds(3), TimeSpan.FromMinutes(5), 0.2);

            // Add dynamic sinks
            queuePaths.ForEach(path => source.RunWith(RestartSink.WithBackoff(() => MsmqSink.Default(sinkSettings, path), restartSettings), materializer));

            latch.Wait();
            sw.Stop();
            Console.WriteLine("Sending [{0}] messages took {1} sec.", nrOfMessages, sw.Elapsed.TotalSeconds);
        }
    }
}