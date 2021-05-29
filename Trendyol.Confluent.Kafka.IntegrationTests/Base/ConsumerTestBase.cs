using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Confluent.Kafka;
using NUnit.Framework;
using Trendyol.Confluent.Kafka.Tests.Helpers;

namespace Trendyol.Confluent.Kafka.Tests.Base
{
    public abstract class ConsumerTestBase : IntegrationTestBase
    {
        private IProducer<string, string> _producer;

        [OneTimeSetUp]
        public override async Task OneTimeSetUp()
        {
            await base.OneTimeSetUp();

            BuildTestProducer();

            await CreateTopic(Constants.TestTopic);
        }

        private void BuildTestProducer()
        {
            var config = new ProducerConfig
            {
                BootstrapServers = Constants.BootstrapServers
            };
            _producer = new ProducerBuilder<string, string>(config)
                .Build();
        }

        protected async Task Produce(string topic, string key, string value)
        {
            await _producer.ProduceAsync(topic, new Message<string, string>
            {
                Key = key,
                Value = value
            });
        }

        private static async Task CreateTopic(string topic, int partitions = 1)
        {
            await AdminClientHelper.CreateTopic(topic, partitions);
        }

        protected static List<ConsumeResult<string, string>> ConsumeAll(string topic)
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = Constants.BootstrapServers,
                GroupId = Guid.NewGuid().ToString(),
                AutoOffsetReset = AutoOffsetReset.Earliest
            };
            using var consumer = new ConsumerBuilder<string, string>(config)
                .Build();
            consumer.Subscribe(topic);

            var records = new List<ConsumeResult<string, string>>();
            while (true)
            {
                var result = consumer.Consume(TimeSpan.FromSeconds(5));

                if (result == null)
                {
                    break;
                }

                records.Add(result);
            }

            return records;
        }
    }
}