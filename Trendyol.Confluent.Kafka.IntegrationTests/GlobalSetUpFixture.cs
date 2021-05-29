using System.Threading.Tasks;
using NUnit.Framework;
using Trendyol.Confluent.Kafka.Tests.Containers;

namespace Trendyol.Confluent.Kafka.Tests
{
    [SetUpFixture]
    public class GlobalSetupFixture
    {
        private KafkaContainer _kafkaContainer;
        private ZookeeperContainer _zookeeperContainer;

        [OneTimeSetUp]
        public async Task OneTimeSetUp()
        {
            _zookeeperContainer = new ZookeeperContainer();
            await _zookeeperContainer.StartAsync();

            _kafkaContainer = new KafkaContainer(_zookeeperContainer.Address);
            await _kafkaContainer.StartAsync();

            await Task.Delay(5000);
        }

        [OneTimeTearDown]
        public async Task OneTimeTearDown()
        {
            var kafka = _kafkaContainer.DisposeAsync();
            var zookeeper = _zookeeperContainer.DisposeAsync();

            await Task.WhenAll(kafka, zookeeper);
        }
    }
}