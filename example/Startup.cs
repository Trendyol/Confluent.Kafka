using System;
using Confluent.Kafka.Lib.Core.Configuration;
using Confluent.Kafka.Lib.Core.Extensions;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;

namespace Confluent.Kafka.Lib.Example
{
    public class Startup
    {
        public void ConfigureServices(IServiceCollection services)
        {
            services.AddHealthChecks();
            
            services.AddKafkaConsumer<TestConsumer>(new KafkaConfig
            {
                Topic = "MY-TOPIC-MAIN",
                RetryTopic = "MY-TOPIC-RETRY",
                FailedTopic = "MY-TOPIC-FAILED",
                RetryPeriod = TimeSpan.FromSeconds(10),
                MainConsumerConfig = new ConsumerConfig
                {
                    GroupId = "dogac-consumer-group",
                    AutoOffsetReset = AutoOffsetReset.Earliest,
                    BootstrapServers = "localhost:9092"
                },
                RetryConsumerConfig = new ConsumerConfig
                {
                    GroupId = "dogac-consumer-group",
                    AutoOffsetReset = AutoOffsetReset.Earliest,
                    BootstrapServers = "localhost:9092",
                },
                MaxRetryCount = 3,
                RetryProducerConfig = new ProducerConfig
                {
                    BootstrapServers = "localhost:9092"
                }
            });
        }

        public void Configure(IApplicationBuilder app)
        {
            app.UseHealthChecks("/healthcheck");
        }
    }
}