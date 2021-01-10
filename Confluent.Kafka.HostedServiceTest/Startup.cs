using System;
using Confluent.Kafka;
using Confluent.Kafka.Utility;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace TestApplication
{
    public class Startup
    {
        private readonly IConfiguration _configuration;

        public Startup(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        public void ConfigureServices(IServiceCollection services)
        {
            services.AddScoped<IService, Service>();

            services.AddHostedService<MyHostedService>();
            
            services.AddKafkaConsumer<MyConsumer>(configuration =>
            {
                configuration.Topics = new[] {"MyTopic"};
                configuration.GroupId = Guid.NewGuid().ToString();
                configuration.BootstrapServers = _configuration.GetValue<string>("BootstrapServers");
                configuration.AutoOffsetReset = AutoOffsetReset.Earliest;
            });

            services.AddSingleton(_configuration);
        }
        
        public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
        {
        }
    }
}