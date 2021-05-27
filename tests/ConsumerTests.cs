using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AdminClientHelpers.Confluent.Kafka;
using Confluent.Kafka;
using FluentAssertions;
using NUnit.Framework;

namespace Trendyol.Confluent.Kafka.Tests
{
    public class ConsumerTests
    {
        private string _currentTopic;
        
        [SetUp]
        public async Task SetUp()
        {
            _currentTopic = Guid.NewGuid().ToString();

            await AdminClientHelper.CreateTopicAsync(Constants.BootstrapServers, _currentTopic, new Random().Next(1, 50));
        }

        [TearDown]
        public async Task TearDown()
        {
            await AdminClientHelper.DeleteTopicAsync(Constants.BootstrapServers, _currentTopic);
        }
        
        private List<Message<string, string>> CreateRandomMessages(int count)
        {
            return Enumerable.Range(0, count)
                .Select(i => new Message<string, string>
                {
                    Key = Guid.NewGuid().ToString(),
                    Value = Guid.NewGuid().ToString()
                })
                .ToList();
        }
        
        private async Task<Message<string, string>> ProduceMessage(string topic,
            Message<string, string> message)
        {
            var config = new ProducerConfig
            {
                BootstrapServers = Constants.BootstrapServers
            };
            var producer = new ProducerBuilder<string, string>(config)
                .Build();
            
            await producer.ProduceAsync(topic, message);

            return message;
        }
        
        private async Task<TestConsumer> StartKafkaConsumer(string topic)
        {
            var consumer = new TestConsumer();
            var configuration = new KafkaConsumerConfig
            {
                Topics = new []{topic},
                BootstrapServers = Constants.BootstrapServers,
                GroupId = Guid.NewGuid().ToString()
            };
            consumer.Initialize(configuration);

            await consumer.RunAsync(CancellationToken.None);

            return consumer;
        }
        
        [Test]
        public async Task RunAsync_GivenSingleMessage_ShouldConsumeMessage()
        {
            var isFaulted = false;
            var exception = null as Exception;
            var consumer = await StartKafkaConsumer(_currentTopic);
            var mre = new ManualResetEvent(false);
            var message = CreateRandomMessages(1).First();
            
            consumer.OnConsumeEvent += result =>
            {
                result.Message.Key.Should().Be(message.Key);
                result.Message.Value.Should().Be(message.Value);
                mre.Set();
            };
            consumer.OnErrorEvent += (e, r) =>
            {
                isFaulted = true;
                exception = e;
                mre.Set();
            };

            // Wait for consumer startup
            await Task.Delay(500);
            
            await ProduceMessage(_currentTopic, message);

            mre.WaitOne();
            
            if (isFaulted)
            {
                Assert.Fail(exception.ToString());
            }
        }
        
        [Test]
        public async Task RunAsync_GivenMultipleMessages_ShouldConsumeMessage()
        {
            var consumer = await StartKafkaConsumer(_currentTopic);
            var isFaulted = false;
            var exception = null as Exception;
            var mre = new ManualResetEvent(false);
            var messages = CreateRandomMessages(50);
            var count = 0;
            
            consumer.OnConsumeEvent += result =>
            {
                messages
                    .Select(m => m.Key)
                    .Should()
                    .Contain(result.Message.Key);
                messages
                    .Select(m => m.Value)
                    .Should()
                    .Contain(result.Message.Value);
                count++;

                if (count == messages.Count)
                {
                    mre.Set();
                }
            };
            consumer.OnErrorEvent += (e, r) =>
            {
                isFaulted = true;
                exception = e;
                mre.Set();
            };

            // Wait for consumer startup
            await Task.Delay(500);

            foreach (var message in messages)
            {
                await ProduceMessage(_currentTopic, message);
            }
            
            mre.WaitOne();

            if (isFaulted)
            {
                Assert.Fail(exception.ToString());
            }
        }
    }
}