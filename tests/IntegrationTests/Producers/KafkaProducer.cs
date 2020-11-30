using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;

namespace Confluent.Kafka.Lib.Tests.IntegrationTests.Producers
{
    public class KafkaProducer : IKafkaProducer
    {
        private readonly IConfiguration _configuration;

        public KafkaProducer(IConfiguration configuration)
        {
            _configuration = configuration;
        }
        
        public async Task ProduceAsync<TKey, TValue>(string topic, Message<TKey, TValue> message)
        {
            var producer = new ProducerBuilder<TKey, TValue>(new ProducerConfig
                {
                    BootstrapServers = _configuration.GetValue<string>("BootstrapServers")
                })
                .Build();
            
            await producer.ProduceAsync(topic, message);
        }
    }
}