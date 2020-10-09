using System;
using System.Text;
using System.Text.Json;

namespace Confluent.Kafka.Lib.Core.Serialization
{
    public class GenericSerializer<T> : ISerializer<T>
    {
        public byte[]? Serialize(T data, SerializationContext context)
        {
            switch (data)
            {
                case double d:
                    return Serializers.Double.Serialize(d, context);
                case int i:
                    return Serializers.Int32.Serialize(i, context);
                case long l:
                    return Serializers.Int64.Serialize(l, context);
                case float f:
                    return Serializers.Single.Serialize(f, context);
                case string s:
                    return Serializers.Utf8.Serialize(s, context);
                case byte[] b:
                    return Serializers.ByteArray.Serialize(b, context);
                case null:
                case Ignore _:
                    return null;
            }

            var serialized = JsonSerializer.Serialize(data);

            return Encoding.UTF8.GetBytes(serialized);
        }
    }
}