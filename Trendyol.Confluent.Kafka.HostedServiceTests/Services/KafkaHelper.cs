using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AdminClientHelpers.Confluent.Kafka;
using Confluent.Kafka;
using Confluent.Kafka.Utility;

namespace TestApplication.Services
{
    public class KafkaHelper : IKafkaHelper
    {
        private KafkaConfiguration _configuration;
        
        public async Task CreateTopic(KafkaConfiguration configuration)
        {
            _configuration = configuration;
            
            var topic = configuration.Topics!.First();

            await AdminClientHelper.CreateTopicAsync(configuration.BootstrapServers,
                topic,
                10);
        }

        public void BeginProducingMessages(KafkaConfiguration configuration)
        {
            new Thread(async () =>
            {
                var producer = new ProducerBuilder<string, string>(new ProducerConfig
                {
                    BootstrapServers = configuration.BootstrapServers
                })
                    .Build();

                while (true)
                {
                    await producer.ProduceAsync(configuration.Topics!.First(), new Message<string, string>
                    {
                        Key = Guid.NewGuid().ToString(),
                        Value = Guid.NewGuid().ToString()
                    });
                }
            }).Start();
        }

        public async Task DeleteTopics()
        {
            await AdminClientHelper.DeleteTopicsAsync(_configuration.BootstrapServers, _configuration.Topics!);
        }
    }
}