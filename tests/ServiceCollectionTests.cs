using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AdminClientHelpers.Confluent.Kafka;
using AutoFixture;
using Confluent.Kafka;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using NUnit.Framework;

namespace Trendyol.Confluent.Kafka.Tests
{
    public class ServiceCollectionTests
    {
        [Test]
        public void Initialize_WhenRetrievedFromRequiredService_ShouldThrowInvalidOperationException()
        {
            var serviceCollection = new ServiceCollection();
            serviceCollection.AddKafkaConsumer<TestConsumer>(configuration =>
            {
                configuration.Topic = "my-topic";
                configuration.GroupId = "my-group-id";
                configuration.BootstrapServers = Constants.BootstrapServers;
            });

            var serviceProvider = serviceCollection.BuildServiceProvider();

            var consumer = serviceProvider.GetRequiredService<TestConsumer>();

            Assert.Throws<InvalidOperationException>(() =>
            {
                consumer.Initialize(default);
            });
        }
        
        [Test]
        public async Task RunAsync_WhenRetrievedFromServices_ShouldRunSuccessfully()
        {
            var topic = Guid.NewGuid().ToString();
            var bootstrapServers = Constants.BootstrapServers;
            
            try
            {
                var serviceCollection = new ServiceCollection();
            
                serviceCollection.AddKafkaConsumer<TestConsumer>(configuration =>
                {
                    configuration.Topic = topic;
                    configuration.GroupId = Guid.NewGuid().ToString();
                    configuration.BootstrapServers = bootstrapServers;
                });
                
                await AdminClientHelper.CreateTopicAsync(bootstrapServers, topic, 1);

                var serviceProvider = serviceCollection.BuildServiceProvider();

                var consumer = serviceProvider.GetRequiredService<TestConsumer>();

                await consumer.RunAsync();

                await Task.Delay(500);
            }
            finally
            {
                await AdminClientHelper.DeleteTopicAsync(bootstrapServers, topic);
            }
        }
        
        [Test]
        public async Task RunAsync_GivenValidConfigurationAndRetrievedFromServices_ShouldConsumeMessage()
        {
            var topic = Guid.NewGuid().ToString();
            var bootstrapServers = Constants.BootstrapServers;
            
            try
            {
                var serviceCollection = new ServiceCollection();
            
                serviceCollection.AddKafkaConsumer<TestConsumer>(configuration =>
                {
                    configuration.Topic = topic;
                    configuration.GroupId = Guid.NewGuid().ToString();
                    configuration.BootstrapServers = bootstrapServers;
                    configuration.AutoOffsetReset = AutoOffsetReset.Earliest;
                });
                
                await AdminClientHelper.CreateTopicAsync(bootstrapServers, topic, 1);

                var serviceProvider = serviceCollection.BuildServiceProvider();

                var consumer = serviceProvider.GetRequiredService<TestConsumer>();
                var mre = new ManualResetEvent(false);
                var message = new Message<string, string>()
                {
                    Key = "my-key",
                    Value = "my-value"
                };

                consumer.OnConsumeEvent += result =>
                {
                    result.Message.Key.Should().Be(message.Key);
                    result.Message.Value.Should().Be(message.Value);
                    
                    mre.Set();
                };

                await consumer.RunAsync();

                var producer = new ProducerBuilder<string, string>(new ProducerConfig
                {
                    BootstrapServers = bootstrapServers
                }).Build();

                await producer.ProduceAsync(topic, message);

                mre.WaitOne();
            }
            finally
            {
                await AdminClientHelper.DeleteTopicAsync(bootstrapServers, topic);
            }
        }
        
        [Test]
        public async Task RunAsync_GivenValidConfigurationAndProducedMultipleMessage_ShouldConsumeMessages()
        {
            var topic = Guid.NewGuid().ToString();
            var bootstrapServers = Constants.BootstrapServers;
            
            try
            {
                var serviceCollection = new ServiceCollection();
            
                serviceCollection.AddKafkaConsumer<TestConsumer>(configuration =>
                {
                    configuration.Topic = topic;
                    configuration.GroupId = Guid.NewGuid().ToString();
                    configuration.BootstrapServers = bootstrapServers;
                    configuration.AutoOffsetReset = AutoOffsetReset.Earliest;
                });
                
                await AdminClientHelper.CreateTopicAsync(bootstrapServers, topic, 1);

                var serviceProvider = serviceCollection.BuildServiceProvider();

                var fixture = new Fixture();
                var consumer = serviceProvider.GetRequiredService<TestConsumer>();
                var mre = new ManualResetEvent(false);
                var messages = fixture
                    .CreateMany<Message<string, string>>(250)
                    .ToList();
                var count = 0;

                consumer.OnConsumeEvent += result =>
                {
                    if (count == messages.Count)
                    {
                        mre.Set();
                    }
                    
                    result.Message.Key
                        .Should()
                        .BeOneOf(messages.Select(m => m.Key));
                    result.Message.Value
                        .Should()
                        .BeOneOf(messages.Select(m => m.Value));
                    
                    mre.Set();
                };

                await consumer.RunAsync();

                var producer = new ProducerBuilder<string, string>(new ProducerConfig
                {
                    BootstrapServers = bootstrapServers
                }).Build();

                foreach (var message in messages)
                {
                    producer.Produce(topic, message);
                }

                producer.Flush(TimeSpan.FromSeconds(5));

                mre.WaitOne();
            }
            finally
            {
                await AdminClientHelper.DeleteTopicAsync(bootstrapServers, topic);
            }
        }
    }
}