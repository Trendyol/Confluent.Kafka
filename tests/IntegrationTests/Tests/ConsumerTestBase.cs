using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using AdminClient.Extensions;
using Confluent.Kafka.Admin;
using Confluent.Kafka.Utility.Tests.IntegrationTests.Consumers;
using Confluent.Kafka.Utility.Tests.IntegrationTests.Fakes;
using Confluent.Kafka.Utility.Tests.IntegrationTests.Producers;
using Microsoft.Extensions.Configuration;
using NUnit.Framework;

namespace Confluent.Kafka.Utility.Tests.IntegrationTests.Tests
{
    public abstract class ConsumerTestBase : IntegrationTestBase
    {
        protected IKafkaProducer Producer;
        protected IConfiguration Configuration;
        protected ConsumerConfig DefaultConfig;
        protected string BootstrapServers;
        protected string Topic;

        [SetUp]
        public virtual async Task SetUp()
        {
            Topic = Guid.NewGuid().ToString();
            BootstrapServers = Configuration.GetValue<string>("BootstrapServers");
            Configuration = GetRequiredService<IConfiguration>();
            Producer = GetRequiredService<IKafkaProducer>();
            DefaultConfig = new ConsumerConfig
            {
                BootstrapServers = BootstrapServers,
                GroupId = Guid.NewGuid().ToString()
            };

            await CreateTopic(Topic);
        }

        [TearDown]
        public virtual async Task TearDown()
        {
            await DeleteTopic(Topic);
            
            Producer = null;
            Configuration = null;
            DefaultConfig = null;
            BootstrapServers = null;
            Topic = null;
        }

        protected ConsumerImpl<TKey, TValue> CreateConsumer<TKey, TValue>(string topic)
        {
            var consumer = new ConsumerBuilder<TKey, TValue>(DefaultConfig)
                .Build();
            return new ConsumerImpl<TKey, TValue>(topic, consumer);
        }

        protected async Task ProduceMessageAsync<TKey, TValue>(TKey key, TValue value)
        {
            await Producer.ProduceAsync<TKey, TValue>(Topic, new Message<TKey, TValue>
            {
                Key = key,
                Value = value
            });
        }

        protected async Task CreateTopic(string topic)
        {
            var exists = AdminClientHelper.TopicExists(BootstrapServers, topic);

            if (exists)
            {
                await AdminClientHelper.DeleteTopicAsync(BootstrapServers, topic);
            }

            await AdminClientHelper.CreateDefaultTopicAsync(BootstrapServers, topic);
        }

        protected async Task DeleteTopic(string topic)
        {
            var exists = AdminClientHelper.TopicExists(BootstrapServers, topic);

            if (!exists)
            {
                return;
            }

            var client = new AdminClientBuilder(new[]
            {
                new KeyValuePair<string, string>("bootstrap.servers", BootstrapServers),
            }).Build();

            await client.DeleteTopicsAsync(new[] {topic}, new DeleteTopicsOptions()
            {
                OperationTimeout = TimeSpan.FromSeconds(10),
                RequestTimeout = TimeSpan.FromSeconds(10),
            });
        }
    }
}