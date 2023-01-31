using System;
using Xunit;

namespace Akka.Streams.Msmq.Tests
{
    public sealed class IgnoreOnGitHubFact : FactAttribute
    {
        public IgnoreOnGitHubFact()
        {
            if (IsGitHubAction())
            {
                Skip = "Ignore test when running on GitHub.";
            }
        }

        private static bool IsGitHubAction()
            => Environment.GetEnvironmentVariable("GITHUB_ACTION") != null;
    }
}