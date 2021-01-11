using System;
using System.Collections.Generic;
using System.Linq;
using FluentAssertions;
using NUnit.Framework;

namespace Trendyol.Confluent.Kafka.Tests.ValidationTests
{
    public class TopicPropertyTests
    {
        [Test]
        public void TopicProperty_GivenValidTopic_ShouldSetAndGetTopicProperty()
        {
            var topic = Guid.NewGuid().ToString();
            var config = new KafkaConfiguration
            {
                Topic = topic
            };

            config.Topic.Should().Be(topic);
        }
        
        [Test]
        public void TopicProperty_GivenNullTopic_ShouldThrowArgumentNullException()
        {
            var exception = Assert.Throws<ArgumentNullException>(() =>
            {
                _ = new KafkaConfiguration
                {
                    Topic = null
                };
            });

            exception.Message.Should().Contain(nameof(KafkaConfiguration.Topic));
        }
        
        [Test]
        public void TopicProperty_GivenNullTopics_TopicShouldBeNull()
        {
            var config = new KafkaConfiguration
            {
                Topics = new[] {null as string}
            };

            config.Topic.Should().BeNull();
        }
        
        [Test]
        public void TopicProperty_GivenValidTopicAndNullTopicsProperty_ShouldSetTopic()
        {
            var config = new KafkaConfiguration
            {
            };

            var topic = "my-topic";
            config.Topic = topic;

            config.Topics.Should().HaveCount(1);
            config.Topics!.First().Should().Be(topic);
        }
        
        [Test]
        public void TopicProperty_GivenTopic_ShouldGetTopics()
        {
            var topic = "myTopic";
            var config = new KafkaConfiguration
            {
                Topic = topic
            };
            config.Topics!.First().Should().Be(topic);
        }
        
        [Test]
        public void TopicProperty_GivenTopics_ShouldGetTopic()
        {
            var topic = "myTopic";
            var config = new KafkaConfiguration
            {
                Topics = new []{topic}
            };
            config.Topic.Should().Be(topic);
        }
    }
}