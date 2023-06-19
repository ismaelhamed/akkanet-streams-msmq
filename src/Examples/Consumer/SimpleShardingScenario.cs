using System;
using System.IO;
using System.Messaging;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Akka;
using Akka.Actor;
using Akka.Cluster.Sharding;
using Akka.Configuration;
using Akka.Pattern;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Streams.Msmq;
using Microsoft.Extensions.Hosting;
using Serilog;
using TitaniumSTG.Messages.HE.DlmsHE;
using Directive = Akka.Streams.Supervision.Directive;

namespace Consumer
{
    public class SimpleShardingScenario : IHostedService
    {
        private ActorSystem actorSystem;

        private static Directive DeserializeMessageDecider(Exception cause) => cause is JsonException
            ? Directive.Resume
            : Directive.Stop;

        public Task StartAsync(CancellationToken cancellationToken)
        {
            Log.Information("Starting actor system...");

            var hoconConfig = ConfigurationFactory.ParseString(File.ReadAllText(AppDomain.CurrentDomain.BaseDirectory + "application.conf"));
            actorSystem = ActorSystem.Create("MsmqExample", hoconConfig);

            Log.Information("Started receiving messages... ");

            var shardRegion = ClusterSharding.Get(actorSystem).Start(
                typeName: "Dummies",
                entityProps: Props.Create<DummyActor>(),
                settings: ClusterShardingSettings.Create(actorSystem),
                messageExtractor: new ShardMessageExtractor(20));

            // Stream will resume (drop current message) in case of a deserialization error
            var deserializeFlow = Flow.Create<Message>()
                .SelectAsync(1, async msg => await JsonSerializer.DeserializeAsync<NotificationResult>(msg.BodyStream, cancellationToken: cancellationToken))
                .WithAttributes(ActorAttributes.CreateSupervisionStrategy(DeserializeMessageDecider));

            var shardingFlowSettings = RestartSettings.Create(TimeSpan.FromSeconds(3), TimeSpan.FromSeconds(3), 0.2);
            var shardingFlow = RestartFlow.WithBackoff(() =>
            {
                return Flow.Create<NotificationResult>()
                    .SelectAsync(1, result =>
                    {
                        return RetrySupport.Retry(() => shardRegion.Ask<Done>(new ShardingEnvelope("1", result), TimeSpan.FromSeconds(5)),
                           3, TimeSpan.FromSeconds(1), actorSystem.Scheduler);
                    });
            }, shardingFlowSettings);

            var sourceSettings = RestartSettings.Create(TimeSpan.FromSeconds(5), TimeSpan.FromMinutes(5), 0.2);
            var source = RestartSource.WithBackoff(() => MsmqSource.Default(MessageQueueSettings.Default, ".\\Private$\\source"), sourceSettings);

            _ = source
                .Via(deserializeFlow)
                .Via(shardingFlow)
                .RunWith(Sink.Ignore<Done>(), ActorMaterializer.Create(actorSystem));

            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            Log.Information("Stopping actor system...");
            return CoordinatedShutdown.Get(actorSystem).Run(CoordinatedShutdown.ClrExitReason.Instance);
        }

        #region Models

        private class DummyActor : UntypedActor
        {
            protected override void OnReceive(object message)
            {
                switch (message)
                {
                    case NotificationResult result:
                        Log.Information("[DummyActor] Received NotificationResult with CorrelationId [{CorrelationId}]", result.CorrelationId);
                        Sender.Tell(Done.Instance);
                        break;
                    default:
                        base.Unhandled(message);
                        break;
                }
            }
        }

        #endregion
    }
}