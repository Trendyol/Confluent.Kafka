using System;
using System.Linq;
using Microsoft.Extensions.DependencyInjection;
using static Trendyol.Confluent.Kafka.ErrorConstants;

namespace Trendyol.Confluent.Kafka
{
    public static class ServiceCollectionExtensions
    {
        public static void AddKafkaConsumer<T>(this IServiceCollection services,
            Action<KafkaConsumerConfig> configurationBuilder,
            ServiceLifetime lifetime = ServiceLifetime.Transient) where T : KafkaConsumer
        {
            if (configurationBuilder == null)
            {
                throw new InvalidOperationException(ConfigurationBuilderCannotBeNullMessage);
            }
            
            var config = new KafkaConsumerConfig();
            configurationBuilder(config);
            var ctor = typeof(T)
                .GetConstructors()
                .First();
            var parameterTypes = ctor
                .GetParameters()
                .Select(p => p.ParameterType)
                .ToArray();
            
            Func<IServiceProvider, T> implementationFactory = provider =>
            {
                var parameters = parameterTypes
                    .Select(t =>
                    {
                        if (t == typeof(KafkaConsumerConfig))
                        {
                            return config as object;
                        }

                        return provider.GetRequiredService<T>();
                    })
                    .ToArray();
                var instance = ctor.Invoke(parameters);
                var castedInstance = instance as T;
                castedInstance!.Initialize(config);
                return castedInstance;
            };
            
            var descriptor = new ServiceDescriptor(typeof(T), implementationFactory, lifetime);
            services.Add(descriptor);
        }
        
        public static void AddKafkaProducer<T>(this IServiceCollection services,
            Action<KafkaProducerConfig> configurationBuilder,
            ServiceLifetime lifetime = ServiceLifetime.Singleton) where T : KafkaProducer
        {
            if (configurationBuilder == null)
            {
                throw new InvalidOperationException(ConfigurationBuilderCannotBeNullMessage);
            }
            
            var config = new KafkaProducerConfig();
            configurationBuilder(config);
            var ctor = typeof(T)
                .GetConstructors()
                .First();
            var parameterTypes = ctor
                .GetParameters()
                .Select(p => p.ParameterType)
                .ToArray();
            
            Func<IServiceProvider, T> implementationFactory = provider =>
            {
                var parameters = parameterTypes
                    .Select(t =>
                    {
                        if (t == typeof(KafkaConsumerConfig))
                        {
                            return config as object;
                        }

                        return provider.GetRequiredService<T>();
                    })
                    .ToArray();
                var instance = ctor.Invoke(parameters);
                var castedInstance = instance as T;
                castedInstance!.Initialize(config);
                return castedInstance;
            };
            
            var descriptor = new ServiceDescriptor(typeof(T), implementationFactory, lifetime);
            services.Add(descriptor);
        }
    }
}