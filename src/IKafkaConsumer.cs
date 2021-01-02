using System.Threading;
using System.Threading.Tasks;

namespace Confluent.Kafka.Utility
{
    public interface IKafkaConsumer
    {
        Task RunAsync(KafkaConfiguration configuration,
            CancellationToken cancellationToken = default);
    }
}