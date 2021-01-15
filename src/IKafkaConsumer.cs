using System;
using System.Threading;
using System.Threading.Tasks;

namespace Trendyol.Confluent.Kafka
{
    public interface IKafkaConsumer : IDisposable
    {
        Task RunAsync(CancellationToken cancellationToken = default);
    }
}