using System.Threading.Tasks;

namespace Trendyol.Confluent.Kafka.HostedServiceTests.Services
{
    public interface IKafkaHelper
    {
        Task CreateTopic(KafkaConfiguration configuration);
        void BeginProducingMessages(KafkaConfiguration configuration);
        Task DeleteTopics();
    }
}