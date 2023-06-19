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

    [Serializable]
    public class HeaderInfo
    {
        public string Key { get; set; }
        public string Value { get; set; }
    }
}

namespace TitaniumSTG.Messages.HE.DlmsHE
{
    public class NotificationResult
    {
        public string CorrelationId { get; set; }
        public string Notification { get; set; }
    }

    public class Notification
    {
        public string CorrelationId { get; set; }
        public bool IsSuccessful { get; set; }
        //public DlmsHE.Shared.Constants.ErrorCode? ErrorCode { get; set; }
        public string ErrorMessage { get; set; }
        //public SystemPrioritiesHE SystemPriority { get; set; }
        //public NotificationTypes NotificationType { get; set; }
        //public Report Payload { get; set; }
        public string DeviceID { get; set; }
        public DateTime? FromDatetime { get; set; }
        public DateTime? ToDatetime { get; set; }
        public string DlmsSpecification { get; set; }
    }
}