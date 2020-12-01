using System.Threading.Tasks;

namespace Confluent.Kafka.Utility.Tests.IntegrationTests.Producers
{
    public interface IKafkaProducer
    {
        Task ProduceAsync<TKey, TValue>(string topic, Message<TKey, TValue> message);
    }
}