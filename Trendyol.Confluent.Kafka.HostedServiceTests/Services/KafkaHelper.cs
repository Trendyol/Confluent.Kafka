using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AdminClientHelpers.Confluent.Kafka;
using Confluent.Kafka;

namespace Trendyol.Confluent.Kafka.HostedServiceTests.Services
{
    public class KafkaHelper : IKafkaHelper
    {
        private KafkaConsumerConfig _consumerConfig;
        
        public async Task CreateTopic(KafkaConsumerConfig consumerConfig)
        {
            _consumerConfig = consumerConfig;
            
            var topic = consumerConfig.Topics!.First();

            await AdminClientHelper.CreateTopicAsync(consumerConfig.BootstrapServers,
                topic,
                10);
        }

        public void BeginProducingMessages(KafkaConsumerConfig consumerConfig)
        {
            new Thread(async () =>
            {
                var producer = new ProducerBuilder<string, string>(new ProducerConfig
                {
                    BootstrapServers = consumerConfig.BootstrapServers
                })
                    .Build();

                while (true)
                {
                    await producer.ProduceAsync(consumerConfig.Topics!.First(), new Message<string, string>
                    {
                        Key = Guid.NewGuid().ToString(),
                        Value = Guid.NewGuid().ToString()
                    });
                }
            }).Start();
        }

        public async Task DeleteTopics()
        {
            await AdminClientHelper.DeleteTopicsAsync(_consumerConfig.BootstrapServers, _consumerConfig.Topics!);
        }
    }
}