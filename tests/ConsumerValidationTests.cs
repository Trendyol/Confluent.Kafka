using System;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using NUnit.Framework;

namespace Confluent.Kafka.Utility.Tests
{
    public class ConsumerTests
    {
        [Test]
        public void RunAsync_GivenNullConfiguration_ShouldThrowArgumentNullException()
        {
            var consumer = new TestConsumer();

            Assert.ThrowsAsync<ArgumentNullException>(async () =>
            {
                await consumer.RunAsync(null, CancellationToken.None);
            });
        }
        
        [Test]
        public void RunAsync_GivenNullTopics_ShouldThrowArgumentNullException()
        {
            var consumer = new TestConsumer();
            var configuration = new KafkaConfiguration
            {
            };

            var exception = Assert.ThrowsAsync<ArgumentNullException>(async () =>
            {
                await consumer.RunAsync(configuration, CancellationToken.None);
            });
            exception.ParamName.Should().Be(nameof(KafkaConfiguration.Topics));
        }
        
        [Test]
        public void RunAsync_GivenTopicsWithNullTopic_ShouldThrowArgumentNullException()
        {
            var consumer = new TestConsumer();
            var configuration = new KafkaConfiguration
            {
                Topics = new []{null as string}
            };

            var exception = Assert.ThrowsAsync<ArgumentNullException>(async () =>
            {
                await consumer.RunAsync(configuration, CancellationToken.None);
            });
            exception.ParamName.Should().Be("topic");
        }
        
        [Test]
        public void RunAsync_GivenNoGroupId_ShouldThrowArgumentException()
        {
            var consumer = new TestConsumer();
            var configuration = new KafkaConfiguration
            {
                Topics = new []{"TestTopic"}
            };

            var exception = Assert.ThrowsAsync<ArgumentException>(async () =>
            {
                await consumer.RunAsync(configuration, CancellationToken.None);
            });
            exception.Message.Should().Contain("'group.id'");
        }
        
        [Test]
        public async Task RunAsync_GivenValidConfiguration_ShouldStartSuccessfully()
        {
            var consumer = new TestConsumer();
            var configuration = new KafkaConfiguration
            {
                Topics = new []{"TestTopic"},
                GroupId = Guid.NewGuid().ToString()
            };
            
            await consumer.RunAsync(configuration, CancellationToken.None);

            await Task.Delay(500);
        }
    }
}