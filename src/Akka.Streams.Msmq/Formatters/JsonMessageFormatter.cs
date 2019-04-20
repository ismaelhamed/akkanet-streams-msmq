using Newtonsoft.Json;
using System.IO;
using System.Runtime.Serialization.Formatters;
using System.Text;

namespace System.Messaging
{
    // https://gist.github.com/jchadwick/2430984
    // https://dejanstojanovic.net/aspnet/2015/october/msmq-json-message-formatter/
    public class JsonMessageFormatter : IMessageFormatter
    {
        public object Clone()
        {
            return new JsonMessageFormatter();
        }

        public bool CanRead(Message message)
        {
            return true;
        }

        public object Read(Message message)
        {
            JsonSerializer jsonSerializer = CreateJsonSerializer();
            JsonReader reader = CreateJsonReader(message.BodyStream);
            var messages = jsonSerializer.Deserialize<Object>(reader);
            return messages;
        }

        public void Write(Message message, object obj)
        {
            Stream stm = new MemoryStream();
            JsonSerializer jsonSerializer = CreateJsonSerializer();
            JsonWriter jsonWriter = CreateJsonWriter(stm);
            jsonSerializer.Serialize(jsonWriter, obj);
            jsonWriter.Flush();
            message.BodyStream = stm;
        }

        private JsonSerializer CreateJsonSerializer()
        {
            var serializerSettings = new JsonSerializerSettings
            {
                TypeNameAssemblyFormat = FormatterAssemblyStyle.Simple,
                TypeNameHandling = TypeNameHandling.Objects
            };

            //serializerSettings.Converters.Add(new MessageJsonConverter());
            return JsonSerializer.Create(serializerSettings);
        }

        protected JsonWriter CreateJsonWriter(Stream stream)
        {
            var streamWriter = new StreamWriter(stream, Encoding.UTF8);
            return new JsonTextWriter(streamWriter) { Formatting = Formatting.Indented };
        }

        protected JsonReader CreateJsonReader(Stream stream)
        {
            var streamReader = new StreamReader(stream, Encoding.UTF8);
            return new JsonTextReader(streamReader);
        }
    }
}
