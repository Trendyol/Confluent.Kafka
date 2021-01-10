using System;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using NUnit.Framework;

namespace Confluent.Kafka.Utility.Tests
{
    public class ConsumerValidationTests
    {
        [Test]
        public void RunAsync_GivenValidConfigurationInConstructor_ShouldStartSuccessfully()
        {
            var configuration = new KafkaConfiguration
            {
                Topics = new [] {Guid.NewGuid().ToString()},
                BootstrapServers = Constants.BootstrapServers,
                GroupId = Guid.NewGuid().ToString()
            };
            var consumer = new TestConsumer(configuration);

            Assert.DoesNotThrowAsync(async () =>
            {
                await consumer.RunAsync();

                await Task.Delay(500);
            });
        }
        
        [Test]
        public void RunAsync_GivenNoParameters_ShouldThrowInvalidOperationException()
        {
            var consumer = new TestConsumer();

            Assert.ThrowsAsync<InvalidOperationException>(async () =>
            {
                await consumer.RunAsync();
            });
        }
        
        [Test]
        public void RunAsync_GivenNullTopics_ShouldThrowArgumentNullException()
        {
            var consumer = new TestConsumer();
            var configuration = new KafkaConfiguration
            {
            };

            var exception = Assert.Throws<ArgumentNullException>(() =>
            {
                consumer.Initialize(configuration);
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

            var exception = Assert.Throws<ArgumentNullException>(() =>
            {
                consumer.Initialize(configuration);
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

            var exception = Assert.ThrowsAsync<InvalidOperationException>(async () =>
            {
                await consumer.RunAsync(CancellationToken.None);
            });
            exception.Message.Should().Be("You have to initialize KafkaConsumer.");
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
            consumer.Initialize(configuration);
            
            await consumer.RunAsync(CancellationToken.None);

            await Task.Delay(500);
        }
    }
}