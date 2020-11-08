using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka.Lib.Core.Configuration;
using Xunit;

namespace Confluent.Kafka.Lib.Tests.Consumers
{
    public class KafkaConsumerTests
    {
        [Fact]
        public void ShouldCreateConsumer()
        {
            var consumer = new TestConsumer();
        }

        [Fact]
        public void ShouldThrowWhenConfigIsNull()
        {
            var consumer = new TestConsumer();

            Assert.Throws<ArgumentNullException>(() => { consumer.SetConfiguration(null); });
        }

        [Fact]
        public void ShouldThrowInvalidConfig()
        {
            var consumer = new TestConsumer();

            Assert.Throws<ArgumentException>(() => { consumer.SetConfiguration(new KafkaConfig()); });
        }

        [Fact]
        public void ShouldThrowWhenBothAutoCommitAndManualCommitIsEnabled()
        {
            var consumer = new TestConsumer();

            var exception = Assert.Throws<ArgumentException>(() =>
            {
                consumer.SetConfiguration(new KafkaConfig()
                {
                    Topic = "TOPIC-1",
                    FailedTopic = "FAILED-TOPIC-1",
                    RetryTopic = "RETRY-TOPIC-1",
                    MainConsumerConfig = new ConsumerConfig(),
                    RetryConsumerConfig = new ConsumerConfig(),
                    RetryProducerConfig = new ProducerConfig(),
                    CommitPeriod = 25,
                });
            });
            Assert.Equal("Both manual commit with commit period and auto commit cannot be set.", exception.Message);
        }

        [Fact]
        public void ShouldThrowWhenNeitherAutoCommitNorCommitPeriodIsEnabled()
        {
            var consumer = new TestConsumer();

            var exception = Assert.Throws<ArgumentException>(() =>
            {
                consumer.SetConfiguration(new KafkaConfig()
                {
                    Topic = "TOPIC-1",
                    FailedTopic = "FAILED-TOPIC-1",
                    RetryTopic = "RETRY-TOPIC-1",
                    MaxRetryCount = 3,
                    MainConsumerConfig = new ConsumerConfig {EnableAutoCommit = false},
                    RetryConsumerConfig = new ConsumerConfig {EnableAutoCommit = false},
                    RetryProducerConfig = new ProducerConfig()
                });
            });
            Assert.Equal("You should either enable auto commit or set commit period.", exception.Message);
        }

        [Fact]
        public async Task ShouldConsumerStart()
        {
            var consumer = new TestConsumer();
            var cts = new CancellationTokenSource();

            consumer.SetConfiguration(new KafkaConfig()
            {
                Topic = "TOPIC-1",
                FailedTopic = "FAILED-TOPIC-1",
                RetryTopic = "RETRY-TOPIC-1",
                MaxRetryCount = 3,
                RetryPeriod = TimeSpan.FromSeconds(10),
                MainConsumerConfig = new ConsumerConfig(),
                RetryConsumerConfig = new ConsumerConfig(),
                RetryProducerConfig = new ProducerConfig()
            });

            await consumer.StartAsync(cts.Token);

            await Task.Delay(5000, cts.Token);
            
            cts.Cancel();
        }
    }
}