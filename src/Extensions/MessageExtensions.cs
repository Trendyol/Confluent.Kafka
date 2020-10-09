using System;
using System.Runtime.CompilerServices;
using static System.StringComparison;

namespace Confluent.Kafka.Lib.Core.Extensions
{
    internal static class MessageExtensions
    {
        /// <summary>
        /// Increments header value, assuming the header value is interpreted as Int32
        /// </summary>
        public static void IncrementHeaderValue<TKey, TValue>(this Message<TKey, TValue> message, string key)
        {
            var value = GetHeaderValue(message, key);
            
            SetHeaderValue(message, key, value + 1);
        }
        
        /// <summary>
        /// Find header and re-interpret its value as T and return it.
        /// </summary>
        public static int GetHeaderValue<TKey, TValue>(this Message<TKey, TValue> message, string key)
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

                    return Unsafe.As<byte, int>(ref bytes[0]);
                }
            }

            return default;
        }

        /// <summary>
        /// Set the header value of given key to value
        /// </summary>
        public static void SetHeaderValue<TKey, TValue>(this Message<TKey, TValue> message, string key, int value)
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

            var bytes = new byte[Unsafe.SizeOf<int>()];

            Unsafe.As<byte, int>(ref bytes[0]) = value;

            message.Headers.Add(key, bytes);
        }
    }
}