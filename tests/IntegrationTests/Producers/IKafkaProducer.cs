using System.Threading.Tasks;

namespace Confluent.Kafka.Lib.Tests.IntegrationTests.Producers
{
    public interface IKafkaProducer
    {
        Task ProduceAsync<TKey, TValue>(string topic, Message<TKey, TValue> message);
    }
}