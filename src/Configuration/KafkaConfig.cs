using System;

namespace Confluent.Kafka.Lib.Core.Configuration
{
    public class KafkaConfig
    {
        /// <summary>
        /// Your main topic to be consumed from.
        /// </summary>
        public string? Topic { get; set; }

        /// <summary>
        /// Your retry topic, records are first produced to your retry topic if processing is failed.
        /// </summary>
        public string? RetryTopic { get; set; }

        /// <summary>
        /// If `MaxRetryCount` is exceeded, records are produced to this topic.
        /// </summary>
        public string? FailedTopic { get; set; }

        /// <summary>
        /// If using manual commit, you can set your commit period from here.
        /// Note that you cannot use CommitPeriod is auto-commit is enabled.
        /// </summary>
        public int? CommitPeriod { get; set; }

        /// <summary>
        /// Maximum retry count for records.
        /// </summary>
        public int? MaxRetryCount { get; set; }

        /// <summary>
        /// Your configuration for main consumer.
        /// </summary>
        public ConsumerConfig? MainConsumerConfig { get; set; }

        /// <summary>
        /// Your configuration for retry consumer.
        /// </summary>
        public ConsumerConfig? RetryConsumerConfig { get; set; }

        /// <summary>
        /// Your configuration for producer for both retry and failed topics.
        /// </summary>
        public ProducerConfig? RetryProducerConfig { get; set; }

        /// <summary>
        /// Set your retry consumer which time period should it periodically run here.
        /// </summary>
        public TimeSpan? RetryPeriod { get; set; }
    }
}