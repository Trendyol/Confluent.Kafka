using Confluent.Kafka;

namespace Trendyol.Confluent.Kafka
{
    public interface IKafkaProducer : IProducer<string, string>
    {
    }
}