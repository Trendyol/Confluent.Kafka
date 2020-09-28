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
            string bootstrapServers,
            string topic,
            string groupId,
            int commitPeriod = 5,
            int maxRetryCount = 3) where T : KafkaConsumer
        {
            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                GroupId = groupId,
            };
            var producerConfig = new ProducerConfig
            {
                BootstrapServers = bootstrapServers,
                Acks = Acks.Leader
            };
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
                var methodInfo = typeof(KafkaConsumer)
                    .GetMethods(BindingFlags.Instance | BindingFlags.NonPublic)
                    .FirstOrDefault(m => m.Name == "SetFields");
                methodInfo?.Invoke(instance, new object[]
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
    }
}