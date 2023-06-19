using System;
using System.Threading.Tasks;
using NServiceBus;
using NServiceBus.Features;
using TitaniumSTG.Messages.HE.DlmsHE;

namespace SampleEndpoint
{
    internal class Program
    {
        public static async Task Main(string[] args)
        {
            Console.Title = "TitaniumSTG.HE.DlmsHE.Mapper";

            // Define the endpoint name
            var endpointConfiguration = new EndpointConfiguration("TitaniumSTG.HE.DlmsHE.Mapper");

            // Select the learning (filesystem-based) transport to communicate with other endpoints
            var transport = endpointConfiguration.UseTransport<MsmqTransport>();
            transport.DisablePublishing();
            transport.Transactions(TransportTransactionMode.ReceiveOnly);

            endpointConfiguration.DisableFeature<TimeoutManager>();

            // Enable monitoring errors, auditing, and heartbeats with the Particular Service Platform tools
            endpointConfiguration.SendFailedMessagesTo("error");
            endpointConfiguration.AuditProcessedMessagesTo("audit");

            // Start the endpoint
            var endpointInstance = await Endpoint.Start(endpointConfiguration)
                .ConfigureAwait(false);

            Console.WriteLine("Press Enter to exit.");
            Console.ReadLine();

            await endpointInstance.Stop()
                .ConfigureAwait(false);
        }

        public class OrdersHandler : IHandleMessages<CommandNotificationResult>
        {
            public Task Handle(CommandNotificationResult message, IMessageHandlerContext context)
            {
                //Console.WriteLine($"Order received {message.NotificationId}");
                return Task.CompletedTask;
            }
        }
    }
}

namespace TitaniumSTG.Messages.HE.DlmsHE
{
    public class CommandNotificationResult
    {
        public Guid NotificationId { get; set; }
    }
}