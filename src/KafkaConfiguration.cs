using System;
using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;

namespace Trendyol.Confluent.Kafka
{
    public class KafkaConfiguration : ConsumerConfig
    {
        public Action<IConsumer<string, string>, Error>? ErrorHandler { get; set; }
        public Action<IConsumer<string, string>, LogMessage>? LogHandler { get; set; }
        public Action<IConsumer<string, string>, string>? StatisticsHandler { get; set; }
        public IDeserializer<string>? KeyDeserializer { get; set; }
        public IDeserializer<string>? ValueDeserializer { get; set; }
        public Func<IConsumer<string, string>, List<TopicPartition>, IEnumerable<TopicPartitionOffset>>?
            PartitionsAssignedHandler { get; set; }
        public Func<IConsumer<string, string>, List<TopicPartitionOffset>, IEnumerable<TopicPartitionOffset>>?
            PartitionsRevokedHandler { get; set; }
        public Action<IConsumer<string, string>, CommittedOffsets>? OffsetsCommittedHandler { get; set; }
        public IEnumerable<string>? Topics { get; set; }

        public string? Topic
        {
            get => Topics?.First(); 
            set
            {
                if (value == null)
                {
                    throw new ArgumentNullException(nameof(Topic));
                }
                
                Topics = new[] {value};
            }
        }
    }
}