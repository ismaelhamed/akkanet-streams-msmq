// Copyright (c) 2023 Ismael Hamed. All rights reserved.
// See LICENSE file in the root folder for full license information.

using System.Threading.Tasks;
using Akka.Actor;
using Akka.Configuration;
using Akka.Util;
using FluentAssertions;
using Xunit;

namespace Akka.Streams.Msmq.Tests.Discovery
{
    public class AkkaDiscoverySpec : Akka.TestKit.Xunit2.TestKit
    {
        [Fact]
        public async Task DiscoverySupport_should_get_addresses_from_configuration()
        {
            using var sys = ActorSystem.Create("AkkaDiscoverySpec", ConfigurationFactory.ParseString($@"            
                akka.discovery {{
                    method = msmq
                    msmq {{
                        class = ""{typeof(MsmqServiceDiscovery).TypeQualifiedName()}""
                    }}
                    config {{
                        services = {{
                            service1 = {{
                                addresses = [
                                    ""FormatName:DIRECT=TCP:157.18.3.1\\Private$\\MyQueue"",
                                    ""FormatName:DIRECT=OS:Mike01\\Private$\\MyOtherQueue""
                                ]
                            }}
                        }}
                    }}
                }}
                alpakka.msmq {{
                    service1 {{
                        service-discovery {{
                            service-name = ""service1""
                            lookup-timeout = 1s
                        }} 
                    }}
                }}")
                .WithFallback(ConfigurationFactory.Load()));

            var config = sys.Settings.Config.GetConfig("alpakka.msmq.service1");
            var addresses = await DiscoverySupport.ReadAddresses(config, sys);
            addresses.Should().BeEquivalentTo(new string[]
            {
                "FormatName:DIRECT=TCP:157.18.3.1\\Private$\\MyQueue",
                "FormatName:DIRECT=OS:Mike01\\Private$\\MyOtherQueue"
            });
        }
    }
}
