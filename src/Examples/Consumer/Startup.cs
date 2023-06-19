using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Messaging;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Serialization;
using Akka;
using Akka.Actor;
using Akka.Cluster.Sharding;
using Akka.Configuration;
using Akka.Pattern;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Streams.Msmq;
using Akka.Util;
using Microsoft.Extensions.Hosting;
using Serilog;
using TitaniumSTG.Messages.HE.DlmsHE;
using Directive = Akka.Streams.Supervision.Directive;

namespace Consumer
{
    public class Startup : IHostedService
    {
        private const string PoisonedQueuePath = ".\\Private$\\TitaniumSTG.HE.DlmsHE.DlmsIn.Poisoned";

        private Config hoconConfig;
        private ActorSystem actorSystem;

        private static readonly XmlSerializer headerSerializer = new XmlSerializer(typeof(List<HeaderInfo>));
        private static readonly XmlMessageFormatter customFormatter = new XmlMessageFormatter(new[] { typeof(NotificationResult) });

        private static Directive DeserializeMessageDecider(Exception cause) => cause is JsonException
            ? Directive.Resume
            : Directive.Stop;

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            Log.Information("Starting actor system...");

            hoconConfig = ConfigurationFactory.ParseString(File.ReadAllText(AppDomain.CurrentDomain.BaseDirectory + "application.conf"));
            actorSystem = ActorSystem.Create("MsmqExample", hoconConfig);

            //Log.Information("Started receiving messages... ");

            //var consumerConfig = hoconConfig.GetConfig("alpakka.msmq.target");
            //var queuePaths = await DiscoverySupport.ReadAddresses(consumerConfig, actorSystem);
            //var materializer = ActorMaterializer.Create(actorSystem);

            ////
            //// Scenarios
            ////

            ////DivertToScenario(queuePaths, materializer);
            //SendToShardingScenario(queuePaths, materializer);

            ////var queue = new MessageQueue(".\\Private$\\DynamicRoutingTest2", false, false, QueueAccessMode.Send);
            ////var formatter = queue.Formatter;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            Log.Information("Stopping actor system...");
            return CoordinatedShutdown.Get(actorSystem).Run(CoordinatedShutdown.ClrExitReason.Instance);
        }

        private static Option<IEnumerable<HeaderInfo>> DeserializeHeaders(Message msg)
        {
            try
            {
                using var ms = new MemoryStream(msg.Extension);
                using var reader = XmlReader.Create(ms);
                return (List<HeaderInfo>)headerSerializer.Deserialize(reader);
            }
            catch (InvalidOperationException ex)
            {
                Log.Warning(ex, "DeserializeHeaders");
                return Option<IEnumerable<HeaderInfo>>.None;
            }
        }

        //private void DivertToScenario(IEnumerable<string> queuePaths, ActorMaterializer materializer)
        //{
        //    static Either<NotificationResult, (ICommittableMessage, Exception)> DeserializeMessage(ICommittableMessage message)
        //    {
        //        try
        //        {
        //            var msg = message.Message;
        //            var headers = DeserializeHeaders(msg);

        //            var settings = new JsonSerializerSettings
        //            {
        //                MissingMemberHandling = MissingMemberHandling.Error,
        //                TypeNameHandling = TypeNameHandling.None // because CA2326
        //            };

        //            // deserialize json message
        //            using var ms = new MemoryStream();
        //            msg.BodyStream.CopyTo(ms);
        //            var notification = JsonConvert.DeserializeObject<NotificationResult>(ByteString.FromBytes(ms.ToArray()).ToString(), settings);

        //            // commit transaction
        //            message.Commit();

        //            return Either.Left(notification);
        //        }
        //        catch (JsonException ex)
        //        {
        //            // NOTE: we do not abort the message at this point so that it can be committed later on after been sent to the DLQ
        //            Log.Warning(ex, "DeserializeMessage");
        //            return Either.Right((message, (Exception)ex));
        //        }
        //    }

        //    var settings = MessageQueueSettings.Default.WithFormatter(customFormatter);

        //    // If reading from Msmq failure is caused by other reasons, like deserialization problems, then the stage will fail immediately.
        //    // If you expect such cases, consider consuming raw byte arrays and deserializing in a subsequent map stage where you can use
        //    // supervision to skip failed elements.

        //    var restartSource = RestartSource.WithBackoff(() => MsmqSource.Committable(settings, queuePaths.First()),
        //        RestartSettings.Create(TimeSpan.FromSeconds(3), TimeSpan.FromSeconds(30), 0.2));

        //    var divertToSink = Flow.Create<Either<NotificationResult, (ICommittableMessage, Exception)>>()
        //        .Collect(either => either.IsRight, either => either.ToRight().Value)
        //        .Select(tuple =>
        //        {
        //            var (msg, _) = tuple;
        //            msg.Commit(); // stop redelivering this message
        //            return msg.Message;
        //        })
        //        .To(MsmqSink.Default(settings, PoisonedQueuePath));

        //    _ = restartSource
        //        .Select(DeserializeMessage)
        //        .DivertToMaterialized(divertToSink, either => either.IsRight, Keep.Left)
        //        .Collect(either => either.IsLeft, either => either.ToLeft().Value)
        //        .RunForeach(result => Log.Information("Received notification [{CorrelationId}]", result.CorrelationId), materializer);
        //}

        private void SendToShardingScenario(IEnumerable<string> queuePaths, ActorMaterializer materializer)
        {
            static async Task<NotificationResult> DeserializeMessage(ICommittableMessage message, CancellationToken cancellationToken = default)
            {
                var msg = message.Message;
                //var headers = DeserializeHeaders(msg);

                //var settings = new JsonSerializerSettings
                //{
                //    MissingMemberHandling = MissingMemberHandling.Error,
                //    TypeNameHandling = TypeNameHandling.None // because CA2326
                //};

                //// deserialize json message
                //using var ms = new MemoryStream();
                //msg.BodyStream.CopyTo(ms);

                //var length = (int)msg.BodyStream.Length;
                //var buffer = ArrayPool<byte>.Shared.Rent(length);

                try
                {
                    //var notification = JsonConvert.DeserializeObject<NotificationResult>(ByteString.FromBytes(ms.ToArray()).ToString(), settings);

                    //_ = await msg.BodyStream.ReadAsync(buffer, 0, length, cancellationToken).ConfigureAwait(false);
                    //var body = buffer.AsMemory(0, length);
                    //var notification = JsonSerializer.Deserialize<NotificationResult>(body.Span);

                    var notification = await JsonSerializer.DeserializeAsync<NotificationResult>(msg.BodyStream);

                    // commit transaction
                    message.Commit();

                    return notification;
                }
                finally
                {
                    //ArrayPool<byte>.Shared.Return(buffer);
                }
            }

            ///// <summary>
            ///// Generate random correlation ID(xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx\xxxxx)
            ///// </summary>
            ///// <returns>string</returns>
            ///// <param name="id">if want specific id in end of generated id, else it will be random forom 1-10000</param>
            //static string GenerateId(int id = 0)
            //{
            //    if (id == 0)
            //    {
            //        var r = new Random(Environment.TickCount);
            //        id = r.Next(1, 10000);
            //        id = r.Next(1, 10000);
            //    }
            //    return $"{Guid.NewGuid()}\\{id}";
            //}

            var shardRegion = ClusterSharding.Get(actorSystem).Start(
                typeName: "Dummies",
                entityProps: Props.Create<DummyActor>(),
                settings: ClusterShardingSettings.Create(actorSystem),
                messageExtractor: new ShardMessageExtractor(20));

            var settings = MessageQueueSettings.Default.WithFormatter(customFormatter);
            var queuePath = ".\\Private$\\DynamicRoutingTest";

            var restartSource = RestartSource.WithBackoff(() => MsmqSource.Committable(settings, queuePath),
                RestartSettings.Create(TimeSpan.FromSeconds(5), TimeSpan.FromMinutes(5), 0.2));

            //var restartSource = MsmqSource.Create(queuePath, settings)
            //    .RunWith(Sink.Ignore<Message>(), materializer);

            //var messages = Enumerable.Range(0, 5)
            //    .Select(i => new Message(i) { CorrelationId = GenerateId() })
            //    .ToList();

            //var source = Source.Cycle(() => messages.GetEnumerator())
            //    .RunWith(Sink.ForEach<Message>(msg => Log.Information("Received message with id [{CorrelationId}]", msg.CorrelationId)), materializer);

            //var restartSource = RestartSource.WithBackoff(() =>
            //    {
            //        //return Source.Cycle(() => messages.GetEnumerator())
            //        return Source.From(messages);
            //            //.SelectAsync(4, result =>
            //            //{
            //            //    return RetrySupport.Retry(() =>
            //            //    {
            //            //        Log.Information("Received message with id [{CorrelationId}]", result.CorrelationId);
            //            //        return shardRegion.Ask<Done>(new ShardingEnvelope(result.CorrelationId, result), TimeSpan.FromSeconds(5));
            //            //    }, 1, TimeSpan.FromSeconds(1), actorSystem.Scheduler);
            //            //});
            //    },
            //    RestartSettings.Create(TimeSpan.FromSeconds(3), TimeSpan.FromSeconds(3), 0.2));

            // Stream will resume (drop current message) in case of a deserialize error
            var deserializeMessage = Flow.Create<ICommittableMessage>()
                .SelectAsync(1, msg => DeserializeMessage(msg))
                .WithAttributes(ActorAttributes.CreateSupervisionStrategy(DeserializeMessageDecider));

            var forwardMessageToSharding = RestartFlow.WithBackoff(() =>
                {
                    return Flow.Create<NotificationResult>()
                        .SelectAsync(1, result =>
                        {
                            return RetrySupport.Retry(() => shardRegion.Ask<Done>(new ShardingEnvelope(result.CorrelationId, result), TimeSpan.FromSeconds(5)),
                                1, TimeSpan.FromSeconds(1), actorSystem.Scheduler);
                        });
                    // TODO: commit/abort message after sharding ack
                },
                RestartSettings.Create(TimeSpan.FromSeconds(3), TimeSpan.FromSeconds(3), 0.2));

            _ = restartSource
                .Via(deserializeMessage)
                .Via(forwardMessageToSharding)
                .RunWith(Sink.Ignore<Done>(), materializer);

            //_ = restartSource
            //    //.Select(DeserializeMessage)
            //    //.Async()
            //    .SelectAsync(4, result =>
            //    {
            //        return RetrySupport.Retry(() =>
            //        {
            //            Log.Information("Received message with id [{CorrelationId}]", result.CorrelationId);
            //            return shardRegion.Ask<Done>(new ShardingEnvelope(result.CorrelationId, result), TimeSpan.FromSeconds(5));
            //        }, 1, TimeSpan.FromSeconds(1), actorSystem.Scheduler);
            //    }) // ResumeOnParsingDecider
            //    .Log("Error")
            //    .RunWith(Sink.Ignore<Done>(), materializer);
        }
    }

    public class DummyActor : ReceiveActor
    {
        public DummyActor() => ReceiveAnyAsync(async msg =>
        {
            var notification = (NotificationResult)msg;
            //Log.Information("Actor {Name} received msg of type [{Type}] with id [{CorrelationId}]",
            //    Self.Path.Name, msg.GetType().Name, notification.CorrelationId);

            Sender.Tell(Done.Instance);

            //var random = new Random();
            //var delay = random.Next(1, 6) * 100;
            //if (delay < 500)
            //{
            //    await Task.Delay(delay);
            //    Sender.Tell(Done.Instance);
            //}
        });
    }

    public class ShardMessageExtractor : HashCodeMessageExtractor
    {
        public ShardMessageExtractor(int maxNumberOfShards)
            : base(maxNumberOfShards)
        { }

        public override string EntityId(object message) => (message as ShardingEnvelope)?.EntityId;
        public override object EntityMessage(object message) => (message as ShardingEnvelope)?.Message;
        public override string ShardId(object message)
        {
            var id = message is ShardRegion.StartEntity se ? se.EntityId : EntityId(message);
            return (Math.Abs(MurmurHash.StringHash(id)) % MaxNumberOfShards).ToString(CultureInfo.InvariantCulture);
        }
    }
}