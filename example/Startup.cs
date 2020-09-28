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
            
            services.AddKafkaConsumer<OmsConsumer>(
                "10.10.36.211:9092",
                "test-topic",
                "test-group-id");
        }

        public void Configure(IApplicationBuilder app)
        {
            app.UseHealthChecks("/healthcheck");
        }
    }
}