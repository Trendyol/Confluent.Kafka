using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace Trendyol.Confluent.Kafka.Tests.Helpers
{
    public class AdminClientHelper
    {
        public static async Task CreateTopic(string topic, int partitions = 1)
        {
            var config = new AdminClientConfig
            {
                BootstrapServers = Constants.BootstrapServers
            };
            var client = new AdminClientBuilder(config)
                .Build();
            await client.CreateTopicsAsync(new[]
            {
                new TopicSpecification
                {
                    Name = topic,
                    NumPartitions = partitions,
                    ReplicationFactor = 1
                }
            });
        }
    }
}