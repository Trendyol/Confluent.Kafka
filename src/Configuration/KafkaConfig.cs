using System;

namespace Confluent.Kafka.Lib.Core.Configuration
{
    public class KafkaConfig
    {
        public string? Topic { get; set; }
        public string? RetryTopic { get; set; }
        public string? FailedTopic { get; set; }
        public int? CommitPeriod { get; set; }
        public int? MaxRetryCount { get; set; }
        public ConsumerConfig? MainConsumerConfig { get; set; }
        public ConsumerConfig? RetryConsumerConfig { get; set; }
        public ProducerConfig? RetryProducerConfig { get; set; }
        public TimeSpan? RetryPeriod { get; set; }
    }
}