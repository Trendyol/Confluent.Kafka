using System;
using System.Linq;
using Microsoft.Extensions.DependencyInjection;
using static Trendyol.Confluent.Kafka.ErrorConstants;

namespace Trendyol.Confluent.Kafka
{
    public static class ServiceCollectionExtensions
    {
        public static void AddKafkaConsumer<T>(this IServiceCollection services,
            Action<KafkaConfiguration> configurationBuilder) where T : KafkaConsumer
        {
            if (configurationBuilder == null)
            {
                throw new InvalidOperationException(ConfigurationBuilderCannotBeNullMessage);
            }
            
            var config = new KafkaConfiguration();
            configurationBuilder(config);
            var ctor = typeof(T)
                .GetConstructors()
                .First();
            var parameterTypes = ctor
                .GetParameters()
                .Select(p => p.ParameterType)
                .ToArray();

            services.AddTransient(provider =>
            {
                var parameters = parameterTypes
                    .Select(provider.GetRequiredService)
                    .ToArray();
                var instance = ctor.Invoke(parameters);
                var castedInstance = instance as T;
                castedInstance!.Initialize(config);
                return castedInstance;
            });
        }
    }
}