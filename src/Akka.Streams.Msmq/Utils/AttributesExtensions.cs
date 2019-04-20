using Akka.Streams.Supervision;

namespace Akka.Streams
{
    public static class AttributesExtensions
    {
        public static Decider GetDeciderOrDefault(this Attributes attributes)
        {
            var attr = attributes.GetAttribute<ActorAttributes.SupervisionStrategy>(null);
            return attr != null ? attr.Decider : Deciders.StoppingDecider;
        }
    }
}
