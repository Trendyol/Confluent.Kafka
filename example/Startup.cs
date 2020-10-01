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
            
            services.AddKafkaConsumer<TestConsumer>(
                "test-topic",
                "test-bootstrap-servers",
                "test-group-id");
        }

        public void Configure(IApplicationBuilder app)
        {
            app.UseHealthChecks("/healthcheck");
        }
    }
}