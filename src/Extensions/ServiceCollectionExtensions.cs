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

            var f = typeof(T)
                .GetRuntimeMethods();

            services.AddTransient<IHostedService, T>(p =>
            {
                var parameters = parameterTypes
                    .Select(t => p.GetRequiredService(t))
                    .ToArray();
                var instance = ctor.Invoke(parameters);
                var setFields = typeof(T)
                    .BaseType
                    .GetMethods(BindingFlags.Instance | BindingFlags.NonPublic)
                    .FirstOrDefault(m => m.Name == "SetFields");
                if (setFields == null)
                {
                    throw new Exception("A private SetFields method must exist to set configuration.");
                }
                
                setFields.Invoke(instance, new object?[]
                {
                    config
                });

                return (T) instance;
            });
        }
    }
}