using System.Threading.Tasks;

namespace Trendyol.Confluent.Kafka.HostedServiceTests.Services
{
    public interface IKafkaHelper
    {
        Task CreateTopic(KafkaConsumerConfig consumerConfig);
        void BeginProducingMessages(KafkaConsumerConfig consumerConfig);
        Task DeleteTopics();
    }
}