using System;
using Confluent.Kafka;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Trendyol.Confluent.Kafka.Tests.Consumers;
using Trendyol.Confluent.Kafka.Tests.Daemons;

namespace Trendyol.Confluent.Kafka.Tests.Base
{
    public class TestStartup
    {
        public static void ConfigureServices(IServiceCollection services)
        {
            services.AddHostedService<IntegrationTestDaemon>();
            
            services.AddKafkaConsumer<IntegrationTestConsumer>(configuration =>
            {
                configuration.Topic = Constants.TestTopic;
                configuration.BootstrapServers = Constants.BootstrapServers;
                configuration.AutoOffsetReset = AutoOffsetReset.Earliest;
                configuration.GroupId = Guid.NewGuid().ToString();
            });
        }

        public static void Configure(IApplicationBuilder app)
        {
        }
    }
}