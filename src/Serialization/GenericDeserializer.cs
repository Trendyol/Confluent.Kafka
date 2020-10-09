using System;
using System.Text.Json;

namespace Confluent.Kafka.Lib.Core.Serialization
{
    public class GenericDeserializer<T> : IDeserializer<T>
    {
        public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            var type = typeof(T);

            if (type == typeof(double))
            {
                var retVal = Deserializers.Double.Deserialize(data, isNull, context);

                return (T) (object) retVal;
            }
            if (type == typeof(float))
            {
                var retVal = Deserializers.Single.Deserialize(data, isNull, context);

                return (T) (object) retVal; 
            }
            if (type == typeof(int))
            {
                var retVal = Deserializers.Int32.Deserialize(data, isNull, context);

                return (T) (object) retVal; 
            }
            if (type == typeof(long))
            {
                var retVal = Deserializers.Int64.Deserialize(data, isNull, context);

                return (T) (object) retVal; 
            }
            if (type == typeof(Null))
            {
                var retVal = Deserializers.Null.Deserialize(data, isNull, context);

                return (T) (object) retVal; 
            }
            if (type == typeof(string))
            {
                var retVal = Deserializers.Utf8.Deserialize(data, isNull, context);

                return (T) (object) retVal; 
            }
            if (type == typeof(byte[]))
            {
                var retVal = Deserializers.ByteArray.Deserialize(data, isNull, context);

                return (T) (object) retVal; 
            }
            if (isNull)
            {
                return default;
            }
            
            return JsonSerializer.Deserialize<T>(data, new JsonSerializerOptions()
            {
                PropertyNameCaseInsensitive = true
            });
        }
    }
}