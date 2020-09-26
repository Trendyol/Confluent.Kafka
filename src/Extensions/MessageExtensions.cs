using System;
using System.Runtime.CompilerServices;
using static System.StringComparison;

namespace Confluent.Kafka.Lib.Core.Extensions
{
    internal static class MessageExtensions
    {
        public static void IncrementHeaderValueAsInt(this Message<string, string> message, string key)
        {
            var value = GetHeaderValue<int>(message, key);
            
            SetHeaderValue(message, key, value + 1);
        }
        
        public static T GetHeaderValue<T>(this Message<string, string> message, string key) where T : unmanaged
        {
            if (message == null)
            {
                throw new ArgumentNullException(nameof(message));
            }
            if (message.Headers == null)
            {
                return default;
            }

            foreach (var header in message.Headers)
            {
                if (header.Key.Equals(key, InvariantCulture))
                {
                    var bytes = header.GetValueBytes();

                    return Unsafe.As<byte, T>(ref bytes[0]);
                }
            }

            return default;
        }

        public static void SetHeaderValue<T>(this Message<string, string> message, string key, T value) where T : unmanaged
        {
            if (message == null)
            {
                throw new ArgumentNullException(nameof(message));
            }

            if (message.Headers == null)
            {
                message.Headers = new Headers();
            }
            
            message.Headers.Remove(key);

            var bytes = new byte[Unsafe.SizeOf<T>()];

            Unsafe.As<byte, T>(ref bytes[0]) = value;

            message.Headers.Add(key, bytes);
        }
    }
}