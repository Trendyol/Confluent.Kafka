using System.Linq;
using System.Reflection;
using Confluent.Kafka.Lib.Core.Consumers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Confluent.Kafka.Lib.Core.Extensions
{
    public static class ServiceCollectionExtensions
    {
        public static void AddKafkaConsumer<T>(this IServiceCollection services,
            string topic,
            string bootstrapServers,
            string groupId) where T : KafkaConsumer
        {
            services.AddKafkaConsumer<T>(topic, 
                bootstrapServers,
                groupId,
                5,
                3);
        }
        
        public static void AddKafkaConsumer<T>(this IServiceCollection services,
            string topic,
            ConsumerConfig consumerConfig,
            ProducerConfig producerConfig,
            int commitPeriod,
            int maxRetryCount) where T : KafkaConsumer
        {
            var ctor = typeof(T)
                .GetConstructors()
                .First();
            var parameterTypes = ctor.GetParameters()
                .Select(p => p.ParameterType);

            services.AddTransient<IHostedService, T>(p =>
            {
                var parameters = parameterTypes
                    .Select(t => p.GetRequiredService(t))
                    .ToArray();
                var instance = ctor.Invoke(parameters);
                var setFieldsMethod = typeof(KafkaConsumer)
                    .GetMethods(BindingFlags.Instance | BindingFlags.NonPublic)
                    .FirstOrDefault(m => m.Name == "SetFields");
                setFieldsMethod?.Invoke(instance, new object[]
                {
                    consumerConfig,
                    producerConfig,
                    topic,
                    commitPeriod,
                    maxRetryCount
                });
                return (T) instance;
            });
        }
        
        public static void AddKafkaConsumer<T>(this IServiceCollection services,
            string topic,
            string bootstrapServers,
            string groupId,
            int commitPeriod,
            int maxRetryCount) where T : KafkaConsumer
        {
            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false,
                GroupId = groupId
            };
            var producerConfig = new ProducerConfig
            {
                BootstrapServers = bootstrapServers,
                Acks = Acks.Leader
            };
            
            services.AddKafkaConsumer<T>(topic, 
                consumerConfig,
                producerConfig,
                commitPeriod,
                maxRetryCount);
        }
    }
}