using System;
using System.Threading;
using System.Threading.Tasks;

namespace Confluent.Kafka.Utility
{
    public interface IKafkaConsumer : IDisposable
    {
        Task RunAsync(CancellationToken cancellationToken);
    }
}