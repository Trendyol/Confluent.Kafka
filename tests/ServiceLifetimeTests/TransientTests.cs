using System;
using System.Threading.Tasks;
using AdminClientHelpers.Confluent.Kafka;
using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using NUnit.Framework;

namespace Trendyol.Confluent.Kafka.Tests.ServiceLifetimeTests
{
    public class TransientTests
    {
        private IProducer<string, string> _producer;

        [SetUp]
        public void SetUp()
        {
            var config = new ProducerConfig()
            {
                BootstrapServers = "localhost:9092"
            };
            _producer = new ProducerBuilder<string, string>(config)
                .Build();
        }
        
        [Test]
        public async Task GivenConsumerWithTransientService_ShouldConsumeMessages()
        {
            var topic = Guid.NewGuid().ToString();
            var groupId = Guid.NewGuid().ToString();
            
            await AdminClientHelper.CreateTopicAsync("localhost:9092", topic, 50);
                
            var services = new ServiceCollection();
            services.AddTransient<TestService>();
            services.AddKafkaConsumer<ConsumerWithTransientField>(configuration =>
            {
                configuration.BootstrapServers = "localhost:9092";
                configuration.Topic = topic;
                configuration.GroupId = groupId;
            });

            var serviceProvider = services.BuildServiceProvider();

            var consumerWithScopedField = serviceProvider.GetRequiredService<ConsumerWithTransientField>();

            await consumerWithScopedField.RunAsync();

            for (int i = 0; i < 250; i++)
            {
                await _producer.ProduceAsync(topic, new Message<string, string>()
                {
                    Key = Guid.NewGuid().ToString(),
                    Value = Guid.NewGuid().ToString()
                });
            }
            
            await AdminClientHelper.DeleteTopicAsync("localhost:9092", topic);
        }
    }
}