using System;
using FluentAssertions;
using NUnit.Framework;

namespace Trendyol.Confluent.Kafka.Tests.ValidationTests
{
    public class ParameterizedCtorTests
    {
        [Test]
        public void GivenValidConfiguration_ShouldCreateTestConsumer()
        {
            var config = new KafkaConsumerConfig
            {
                Topics = new[] {"MyTopic"},
                GroupId = "groupId"
            };
            _ = new TestConsumer(config);
        }
        
        [Test]
        public void GivenNoGroupId_ShouldThrowArgumentException()
        {
            var config = new KafkaConsumerConfig
            {
                Topics = new[] {"MyTopic"}
            };
            var exception = Assert.Throws<ArgumentException>(() =>
            {
                _ = new TestConsumer(config);
            });
            exception.Message.Should().Contain("'group.id'");
        }
        
        [Test]
        public void GivenNoTopics_ShouldThrowArgumentNullException()
        {
            var config = new KafkaConsumerConfig
            {
                Topics = null,
                GroupId = "groupId"
            };
            var exception = Assert.Throws<ArgumentNullException>(() =>
            {
                _ = new TestConsumer(config);
            });
            exception.Message.Should().Contain(nameof(KafkaConsumerConfig.Topics));
        }
        
        [Test]
        public void GivenNoTopicInTopics_ShouldThrowArgumentNullException()
        {
            var config = new KafkaConsumerConfig
            {
                Topics = new []{null as string},
                GroupId = "groupId"
            };
            var exception = Assert.Throws<ArgumentNullException>(() =>
            {
                _ = new TestConsumer(config);
            });
            exception.Message.Should().Contain("topic");
        }
        
        [Test]
        public void GivenNullConfig_ShouldThrowArgumentNullException()
        {
            var exception = Assert.Throws<ArgumentNullException>(() =>
            {
                _ = new TestConsumer(null);
            });
            exception.Message.Should().Contain(nameof(KafkaConsumerConfig));
        }
        
        [Test]
        public void GivenConfigWithDefaultValues_ShouldThrowArgumentNullException()
        {
            Assert.Throws<ArgumentNullException>(() =>
            {
                _ = new TestConsumer(default);
            });
        }
    }
}