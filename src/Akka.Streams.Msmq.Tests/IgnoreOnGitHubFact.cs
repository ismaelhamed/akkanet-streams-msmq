using System;
using Xunit;

namespace Akka.Streams.Msmq.Tests
{
    public sealed class IgnoreOnGitHubFact : FactAttribute
    {
        public IgnoreOnGitHubFact()
        {
            if (Environment.GetEnvironmentVariable("GITHUB_ACTIONS") == "true")
            {
                Skip = "Ignore test when running on GitHub.";
            }
        }
    }
}