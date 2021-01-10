using System.Threading;
using System.Threading.Tasks;

namespace Trendyol.Confluent.Kafka
{
    public interface IKafkaConsumer
    {
        Task RunAsync(CancellationToken cancellationToken = default);
    }
}