using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using NUnit.Framework;

namespace Confluent.Kafka.Utility.Tests
{
    public class ConsumerTests
    {
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
            var configuration = new KafkaConfiguration
            {
                Topics = new []{"MyTopic"},
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
            var topic = "MyTopic";
            var consumer = await StartKafkaConsumer(topic);
            var mre = new ManualResetEvent(false);
            var message = CreateRandomMessages(1).First();
            
            consumer.OnConsumeEvent += result =>
            {
                result.Message.Key.Should().Be(message.Key);
                result.Message.Value.Should().Be(message.Value);
                mre.Set();
            };
            consumer.OnErrorEvent += (exception, result) =>
            {
                Assert.Fail(exception.ToString());
            };

            // Wait for consumer startup
            await Task.Delay(500);
            
            await ProduceMessage(topic, message);

            mre.WaitOne();
        }
        
        [Test]
        public async Task RunAsync_GivenMultipleMessages_ShouldConsumeMessage()
        {
            var topic = "MyTopic";
            var consumer = await StartKafkaConsumer(topic);
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

                if (count == 50)
                {
                    mre.Set();
                }
            };
            consumer.OnErrorEvent += (exception, result) =>
            {
                Assert.Fail(exception.ToString());
            };

            // Wait for consumer startup
            await Task.Delay(500);

            foreach (var message in messages)
            {
                await ProduceMessage(topic, message);
            }

            mre.WaitOne();
        }
    }
}