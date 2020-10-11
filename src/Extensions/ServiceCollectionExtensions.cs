using System;
using System.Linq;
using System.Reflection;
using Confluent.Kafka.Lib.Core.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Confluent.Kafka.Lib.Core.Extensions
{
    public static class ServiceCollectionExtensions
    {
        public static void AddKafkaConsumer<T>(this IServiceCollection services,
            KafkaConfig config) where T : BackgroundService 
        {
            var ctor = typeof(T)
                .GetConstructors()
                .First();
            var parameterTypes = ctor
                .GetParameters()
                .Select(p => p.ParameterType)
                .ToList();

            services.AddTransient<IHostedService, T>(p =>
            {
                var parameters = parameterTypes
                    .Select(p.GetRequiredService)
                    .ToArray();
                var instance = ctor.Invoke(parameters);
                var setConfig = typeof(T)
                    .BaseType
                    .GetMethods(BindingFlags.Instance | BindingFlags.NonPublic)
                    .FirstOrDefault(m => m.Name == "SetConfiguration");
                if (setConfig == null)
                {
                    throw new Exception("A private SetConfiguration method must exist to set configuration.");
                }
                
                setConfig.Invoke(instance, new object?[]
                {
                    config
                });

                return (T) instance;
            });
        }
    }
}