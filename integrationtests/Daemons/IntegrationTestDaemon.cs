using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Trendyol.Confluent.Kafka.Tests.Consumers;

namespace Trendyol.Confluent.Kafka.Tests.Daemons
{
    public class IntegrationTestDaemon : BackgroundService
    {
        private readonly IntegrationTestConsumer _consumer;

        public IntegrationTestDaemon(IntegrationTestConsumer consumer)
        {
            _consumer = consumer;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            await _consumer.RunAsync(stoppingToken);
        }
    }
}