using System;
using FluentAssertions;
using NUnit.Framework;

namespace Trendyol.Confluent.Kafka.Tests.ValidationTests
{
    public class InitializeTests
    {
        [Test]
        public void Initialize_GivenNoGroupId_ShouldThrowArgumentException()
        {
            var config = new KafkaConfiguration
            {
                Topics = new[] {"MyTopic"}
            };
            var consumer = new TestConsumer();

            var exception = Assert.Throws<ArgumentException>(() =>
            {
                consumer.Initialize(config);
            });
            exception.Message.Should().Contain("'group.id'");
        }
        
        [Test]
        public void Initialize_GivenNoTopics_ShouldThrowArgumentNullException()
        {
            var config = new KafkaConfiguration
            {
            };
            var consumer = new TestConsumer();

            var exception = Assert.Throws<ArgumentNullException>(() =>
            {
                consumer.Initialize(config);
            });
            exception.Message.Should().Contain(nameof(KafkaConfiguration.Topics));
        }
        
        [Test]
        public void Initialize_GivenNoTopicInTopics_ShouldThrowArgumentNullException()
        {
            var config = new KafkaConfiguration
            {
                Topics = new [] {null as string}
            };
            var consumer = new TestConsumer();

            var exception = Assert.Throws<ArgumentNullException>(() =>
            {
                consumer.Initialize(config);
            });
            exception.Message.Should().Contain("topic");
        }
        
        [Test]
        public void Initialize_IfAlreadyInitialized_ShouldThrowInvalidOperationException()
        {
            var config = new KafkaConfiguration
            {
                GroupId = "groupId",
                Topics = new []{"topic"}
            };
            var consumer = new TestConsumer();
            consumer.Initialize(config);

            var exception = Assert.Throws<InvalidOperationException>(() =>
            {
                consumer.Initialize(config);
            });
            exception.Message.Should().Be("KafkaConsumer is already initialized.");
        }
        
        [Test]
        public void Initialize_IfAlreadyInitializedWithCtor_ShouldThrowInvalidOperationException()
        {
            var config = new KafkaConfiguration
            {
                GroupId = "groupId",
                Topics = new []{"topic"}
            };
            var consumer = new TestConsumer(config);

            var exception = Assert.Throws<InvalidOperationException>(() =>
            {
                consumer.Initialize(config);
            });
            exception.Message.Should().Be("KafkaConsumer is already initialized.");
        }
    }
}