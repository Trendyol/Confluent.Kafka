using System.Threading.Tasks;
using Confluent.Kafka.Utility;

namespace TestApplication.Services
{
    public interface IKafkaHelper
    {
        Task CreateTopic(KafkaConfiguration configuration);
        void BeginProducingMessages(KafkaConfiguration configuration);
        Task DeleteTopics();
    }
}