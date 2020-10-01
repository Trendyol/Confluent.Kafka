using System;
using System.Runtime.CompilerServices;
using static System.StringComparison;

namespace Confluent.Kafka.Lib.Core.Extensions
{
    internal static class MessageExtensions
    {
        /// <summary>
        /// Increments header value, assuming the header value is
        /// re-interpreted as Int32
        /// </summary>
        /// <param name="message"></param>
        /// <param name="key"></param>
        public static void IncrementHeaderValueAsInt(this Message<string, string> message, string key)
        {
            var value = GetHeaderValue<int>(message, key);
            
            SetHeaderValue(message, key, value + 1);
        }
        
        /// <summary>
        /// Find header and re-interpret its value as T and return it.
        /// </summary>
        /// <param name="message"></param>
        /// <param name="key"></param>
        /// <typeparam name="T">The type that message value will be interpreted as.</typeparam>
        /// <returns>Message value as T</returns>
        /// <exception cref="ArgumentNullException"></exception>
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

        /// <summary>
        /// Set the header value of given key to value
        /// </summary>
        /// <param name="message"></param>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <typeparam name="T"></typeparam>
        /// <exception cref="ArgumentNullException"></exception>
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