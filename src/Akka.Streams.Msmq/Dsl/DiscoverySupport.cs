// Copyright (c) 2023 Ismael Hamed. All rights reserved.
// See LICENSE file in the root folder for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Configuration;
using Akka.Discovery;
using Akka.Event;

namespace Akka.Streams.Msmq
{
    /// <summary>
    /// Reads MSMQ addresses from configured sources via
    /// <see href="https://getakka.net/articles/discovery/index.html">Akka.Discovery</see> configuration.
    /// <para>
    /// Example using a custom config-based ServiceDiscovery:
    /// </para>
    /// <code>
    /// akka.discovery {
    ///   method = msmq
    ///   msmq {
    ///     class = "Akka.Streams.Msmq.Discovery.MsmqServiceDiscovery, Akka.Streams.Msmq"
    ///   }
    ///   config {
    ///     services = {
    ///       service1 = {
    ///         addresses = [
    ///           "FormatName:DIRECT=TCP:157.18.3.1\\Private$\\MyQueue",
    ///           "FormatName:DIRECT=OS:Mike01\\Private$\\MyOtherQueue"
    ///         ]
    ///       }
    ///       service2 = {
    ///         addresses = [
    ///           ".\\Private$\\MyQueue",
    ///         ]
    ///       }
    ///     }
    ///   }
    /// }
    ///
    /// alpakka.msmq {
    ///   service1 {
    ///     service-discovery {
    ///       service-name = "service1"
    ///       lookup-timeout = 1s
    ///     }
    ///   }
    /// }
    /// </code>
    /// <para>Look up `service1` using `DiscoverySupport`:</para>
    /// <code>
    /// var config = hocon.GetConfig("alpakka.msmq.service1");
    /// var addresses = (await DiscoverySupport.ReadAddresses(config, actorSystem)).ToArray();
    /// </code>
    /// </summary>
    public sealed class DiscoverySupport
    {
        /// <summary>
        /// Expect a `service-discovery` section in Config and use Akka Discovery to read the addresses for `service-name` within `lookup-timeout`.
        /// </summary>
        /// <param name="config">A configuration object.</param>
        /// <param name="system">The ActorSystem.</param>
        public static Task<IEnumerable<string>> ReadAddresses(Config config, ActorSystem system)
        {
            if (config.HasPath("service-discovery"))
            {
                var serviceConfig = config.GetConfig("service-discovery");
                var serviceName = serviceConfig.GetString("service-name");
                var lookupTimeout = serviceConfig.GetTimeSpan("lookup-timeout");
                return ReadAddresses(serviceName, lookupTimeout, system);
            }

            throw new ArgumentException($"Config {config} does not contain `service-discovery` section");
        }

        /// <summary>
        /// Use Akka Discovery to read the addresses for <paramref name="serviceName"/> within <paramref name="lookupTimeout"/>.
        /// </summary>
        private static async Task<IEnumerable<string>> ReadAddresses(string serviceName, TimeSpan lookupTimeout, ActorSystem system)
        {
            var discovery = Discovery.Discovery.Get(system).Default;
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

        public override Task<Resolved> Lookup(Lookup lookup, TimeSpan resolveTimeout) =>
            Task.FromResult(!_resolvedServices.TryGetValue(lookup.ServiceName, out var resolved)
                ? new Resolved(lookup.ServiceName, null)
                : resolved);

        private class MsmqServicesParser
        {
            public static Dictionary<string, Resolved> Parse(Config config) =>
                config.AsEnumerable()
                    .Select(pair => (pair.Key, config.GetConfig(pair.Key)))
                    .ToDictionary(pair => pair.Key, pair =>
                    {
                        var (serviceName, full) = pair;
                        var endpoints = full.GetStringList("addresses");
                        var resolvedTargets = endpoints.Select(path => new ResolvedTarget(path)).ToArray();
                        return new Resolved(serviceName, resolvedTargets);
                    });
        }
    }
}