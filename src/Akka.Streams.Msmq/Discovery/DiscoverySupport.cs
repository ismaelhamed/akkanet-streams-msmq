using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Configuration;
using Akka.Discovery;
using Akka.Event;

namespace Akka.Streams.Msmq.Discovery
{
    /// <summary>
    /// Reads MSMQ addresses from configured sources via <see cref="Akka.Discovery.Discovery"/> configuration.
    /// </summary>
    public sealed class DiscoverySupport
    {
        /// <summary>
        /// Expect a `service-discovery` section in Config and use Akka Discovery to read the addresses for `service-name` within `lookup-timeout`.
        /// </summary>
        public static Task<IEnumerable<string>> ReadAddresses(Config config, ActorSystem system)
        {
            if (config.HasPath("service-discovery"))
            {
                var serviceName = config.GetString("service-discovery.service-name");
                var lookupTimeout = config.GetTimeSpan("service-discovery.lookup-timeout");
                return ReadAddresses(serviceName, lookupTimeout, system);
            }
            else
            {
                throw new ArgumentException($"Config {config} does not contain `service-discovery` section");
            }
        }

        /// <summary>
        /// Use Akka Discovery to read the addresses for <paramref name="serviceName"/> within <paramref name="lookupTimeout"/>.
        /// </summary>
        private static async Task<IEnumerable<string>> ReadAddresses(string serviceName, TimeSpan lookupTimeout, ActorSystem system)
        {
            var discovery = Akka.Discovery.Discovery.Get(system).LoadServiceDiscovery("msmq");
            var resolved = await discovery.Lookup(serviceName, lookupTimeout).ConfigureAwait(false);
            return resolved.Addresses.Select(a => a.Host);
        }
    }

    public class MsmqServiceDiscovery : ServiceDiscovery
    {
        private readonly Dictionary<string, Resolved> _resolvedServices;

        public MsmqServiceDiscovery(ExtendedActorSystem system)
        {
            _resolvedServices = MsmqServicesParser.Parse(
                system.Settings.Config.GetConfig(system.Settings.Config.GetString("akka.discovery.config.services-path")));

            var log = Logging.GetLogger(system, nameof(MsmqServiceDiscovery));
            log.Debug($"Config discovery serving: {string.Join(", ", _resolvedServices.Values)}");
        }

        public override Task<Resolved> Lookup(Lookup lookup, TimeSpan resolveTimeout)
        {
            return Task.FromResult(!_resolvedServices.TryGetValue(lookup.ServiceName, out var resolved)
                ? new Resolved(lookup.ServiceName, null)
                : resolved);
        }

        private class MsmqServicesParser
        {
            public static Dictionary<string, Resolved> Parse(Config config)
            {
                return config.AsEnumerable()
                    .Select(pair => (pair.Key, config.GetConfig(pair.Key)))
                    .ToDictionary(pair => pair.Key, pair =>
                    {
                        var (serviceName, full) = pair;
                        var endpoints = full.GetStringList("endpoints");
                        var resolvedTargets = endpoints.Select(path => new ResolvedTarget(path)).ToArray();
                        return new Resolved(serviceName, resolvedTargets);
                    });
            }
        }
    }
}