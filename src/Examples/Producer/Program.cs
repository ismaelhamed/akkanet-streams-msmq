using System;
using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Serilog;
using Serilog.Events;
using Serilog.Exceptions;

namespace Producer
{
    internal class Program
    {
        private static int Main(string[] args)
        {
            var assemblyName = Assembly.GetExecutingAssembly().GetName();
            Log.Logger = new LoggerConfiguration()
                .MinimumLevel.Debug()
                .MinimumLevel.Override("Microsoft", LogEventLevel.Warning)
                .Enrich.FromLogContext()
                .Enrich.WithExceptionDetails()
                .Enrich.WithMachineName()
                .Enrich.WithProperty("Assembly", $"{assemblyName.Name}")
                .Enrich.WithProperty("Version", $"{assemblyName.Version}")
                .WriteTo.Console()
                .WriteTo.Seq("http://localhost:5341")
                .CreateLogger();

            try
            {
                Log.Information("Starting host...");
                CreateHostBuilder(args).Build().Run();
                return 0;
            }
            catch (Exception ex)
            {
                Log.Fatal(ex, "Host terminated unexpectedly");
                return 1;
            }
            finally
            {
                Log.CloseAndFlush();
            }
        }

        public static IHostBuilder CreateHostBuilder(string[] _) =>
            new HostBuilder()
                .ConfigureServices(services =>
                {
                    services.Configure<ConsoleLifetimeOptions>(options => options.SuppressStatusMessages = true);
                    //services.AddHostedService<Startup>();
                    services.AddHostedService<RoundRobinScenario>();
                })
                .UseWindowsService()
                .UseSerilog();
    }
}