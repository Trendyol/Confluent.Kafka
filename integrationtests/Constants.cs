using System;

namespace Trendyol.Confluent.Kafka.Tests
{
    public static class Constants
    {
        public const string BootstrapServers = "localhost:9092";

        public static readonly string TestTopic = Guid.NewGuid().ToString();
    }
}