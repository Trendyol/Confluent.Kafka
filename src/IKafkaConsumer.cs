using System;
using System.Threading;
using System.Threading.Tasks;

namespace Confluent.Kafka.Lib.Core
{
    public interface IKafkaConsumer : IDisposable
    {
        Task RunAsync(CancellationToken cancellationToken);
    }
}