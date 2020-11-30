using Confluent.Kafka.Lib.Tests.IntegrationTests.Producers;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;

namespace Confluent.Kafka.Lib.Tests.IntegrationTests.Fakes
{
    public class TestStartup
    {
        public void ConfigureServices(IServiceCollection services)
        {
            services.AddHealthChecks();

            services.AddSingleton<IKafkaProducer, KafkaProducer>();
        }

        public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
        {
            
            app.UseHealthChecks("/healthcheck");
        }
    }
}