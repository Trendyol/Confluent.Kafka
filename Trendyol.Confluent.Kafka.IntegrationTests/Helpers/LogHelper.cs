using System;

namespace Trendyol.Confluent.Kafka.Tests.Helpers
{
    public static class LogHelper
    {
        public const string AnErrorOccurredLog = "An error occurred";
        
        public static string CreateEventConsumedLog(string key, string value)
        {
            return $"Key: {key}, Value: {value} is consumed.";
        }

        public static string CreateAnErrorOccurredLog(Exception exception, string key, string value)
        {
            return $"{AnErrorOccurredLog}: {exception} with " +
                   $"Key: {key}, " +
                   $"Value: {value}.";
        }
    }
}