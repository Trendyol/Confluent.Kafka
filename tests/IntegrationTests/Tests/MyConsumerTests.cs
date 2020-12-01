using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using AdminClient.Extensions;
using AutoFixture;
using Confluent.Kafka.Admin;
using Confluent.Kafka.Utility.Tests.IntegrationTests.Consumers;
using Confluent.Kafka.Utility.Tests.IntegrationTests.Fakes;
using Confluent.Kafka.Utility.Tests.IntegrationTests.Producers;
using FluentAssertions;
using Microsoft.Extensions.Configuration;
using NUnit.Framework;

namespace Confluent.Kafka.Utility.Tests.IntegrationTests.Tests
{
    public class MyConsumerTests : IntegrationTestBase
    {
        private IFixture _fixture;
        private IKafkaProducer _producer;
        private IConfiguration _configuration;
        private ConsumerConfig _defaultConfig;
        private string _bootstrapServers;
        private string _topic;

        [SetUp]
        public async Task SetUp()
        {
            _fixture = new Fixture();
            _producer = GetRequiredService<IKafkaProducer>();
            _configuration = GetRequiredService<IConfiguration>();
            _bootstrapServers = _configuration.GetValue<string>("BootstrapServers");
            _defaultConfig = new ConsumerConfig
            {
                BootstrapServers = _bootstrapServers,
                GroupId = Guid.NewGuid().ToString()
            };
            _topic = Guid.NewGuid().ToString();
            await CreateTopic(_topic);
        }

        [TearDown]
        public async Task TearDown()
        {
            await DeleteTopic(_topic);
            _topic = null;
        }

        private ConsumerImpl<TKey, TValue> CreateConsumer<TKey, TValue>(string topic)
        {
            var consumer = new ConsumerBuilder<TKey, TValue>(_defaultConfig)
                .Build();
            return new ConsumerImpl<TKey, TValue>(topic, consumer);
        }

        private async Task ProduceMessageAsync<TKey, TValue>(TKey key, TValue value)
        {
            await _producer.ProduceAsync<TKey, TValue>(_topic, new Message<TKey, TValue>
            {
                Key = key,
                Value = value
            });
        }
        
        private async Task CreateTopic(string topic)
        {
            var exists = AdminClientHelper.TopicExists(_bootstrapServers, topic);

            if (exists)
            {
                await AdminClientHelper.DeleteTopicAsync(_bootstrapServers, topic);
            }

            await AdminClientHelper.CreateDefaultTopicAsync(_bootstrapServers, topic);
        }
        
        private async Task DeleteTopic(string topic)
        {
            var exists = AdminClientHelper.TopicExists(_bootstrapServers, topic);

            if (!exists)
            {
                return;
            }
            
            var client = new AdminClientBuilder(new []
            {
                new KeyValuePair<string, string>("bootstrap.servers", _bootstrapServers), 
            }).Build();

            await client.DeleteTopicsAsync(new[] {topic}, new DeleteTopicsOptions()
            {
                OperationTimeout = TimeSpan.FromSeconds(10),
                RequestTimeout = TimeSpan.FromSeconds(10),
            });
        }

        [Test]
        public async Task RunAsync_WhenValidRecordProduced_ShouldConsumeRecord()
        {
            var key = _fixture.Create<string>();
            var value = _fixture.Create<string>();
            var mre = new ManualResetEvent(false);
            
            var processedRecord = null as ConsumeResult<string, string>;

            using var consumer = CreateConsumer<string, string>(_topic);

            await consumer.RunAsync();
            
            await Task.Delay(500);

            consumer.RecordProcessed += result =>
            {
                processedRecord = result;
                mre.Set();
            };

            await ProduceMessageAsync(key, value);

            mre.WaitOne();

            processedRecord.Message.Key.Should().Be(key);
            processedRecord.Message.Value.Should().Be(value);
        }
        
        [Test]
        public async Task RunAsync_WhenDoubleMessageProducedAndStringIsConsumed_ShouldNotGiveConsumeError()
        {
            var key = _fixture.Create<double>();
            var value = _fixture.Create<double>();

            using var consumer = CreateConsumer<string, string>(_topic);
            
            await consumer.RunAsync();

            await Task.Delay(500);

            consumer.RecordProcessed += result =>
            {
                Assert.Pass();
            };
            consumer.OnConsumeErrored += result =>
            {
                Assert.Fail();
            };

            await _producer.ProduceAsync(_topic, new Message<double, double>
            {
                Key = key,
                Value = value
            });
        }
        
        [Test]
        public async Task RunAsync_WhenLongMessageProduced_ShouldConsumeRecord()
        {
            var key = _fixture.Create<long>();
            var value = _fixture.Create<long>();

            using var consumer = CreateConsumer<long, long>(_topic);

            await consumer.RunAsync();

            await Task.Delay(500);
            
            var mre = new ManualResetEvent(false);

            consumer.RecordProcessed += result =>
            {
                result.Should().NotBeNull();
                result.Message.Key.Should().Be(key);
                result.Message.Value.Should().Be(value);
                mre.Set();
            };
            consumer.OnConsumeErrored += result =>
            {
                Assert.Fail();
            };
            consumer.OnCommitErrored += (exception, result) =>
            {
                Assert.Fail();
            };
            consumer.OnProcessErrored += (exception, result) =>
            {
                Assert.Fail();
            };

            await ProduceMessageAsync(key, value);

            mre.WaitOne();
        }
        
        [Test]
        public async Task RunAsync_WhenCancellationTokenGiven_ShouldEndWhenTokenCancelled()
        {
            var key = _fixture.Create<string>();
            var value = _fixture.Create<string>();
            using var consumer = CreateConsumer<long, long>(_topic);
            
            var cts = new CancellationTokenSource();

            await consumer.RunAsync(cts.Token);

            await Task.Delay(500, cts.Token);

            consumer.RecordProcessed += result =>
            {
                Assert.Fail();
            };
            consumer.OnConsumeErrored += result =>
            {
                Assert.Fail();
            };
            consumer.OnCommitErrored += (exception, result) =>
            {
                Assert.Fail();
            };
            consumer.OnProcessErrored += (exception, result) =>
            {
                Assert.Fail();
            };
            
            cts.Cancel();

            for (var i = 0; i < 10; i++)
            {
                await ProduceMessageAsync(key, value);
            }

            // ReSharper disable once MethodSupportsCancellation
            await Task.Delay(1000);

            Assert.Pass();
        }
    }
}