using System;

namespace Consumer
{
    public class Device
    {
        public Guid DeviceId { get; set; }
        public DateTime EventTime { get; set; }
    }

    public class Device2
    {
        public string Id { get; set; }
    }
}