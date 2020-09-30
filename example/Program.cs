using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NLog.Web;

namespace Confluent.Kafka.Lib.Example
{
    public static class Program
    {
        public static void Main(string[] args)
        {
            Task.Delay(TimeSpan.FromSeconds(5))
                .ContinueWith(async _ =>
                {
                    await ContinuationAction(_);
                });
            
            IConfigurationBuilder builder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json");

            var configuration = builder.Build();

            Host.CreateDefaultBuilder(args)
                .ConfigureWebHostDefaults(webBuilder =>
                {
                    webBuilder.ConfigureLogging(l =>
                    {
                        l.ClearProviders();
                        l.AddConsole();
                        l.SetMinimumLevel(LogLevel.Information);
                    });

                    webBuilder.UseConfiguration(configuration);
                    webBuilder.UseStartup<Startup>();
                    webBuilder.UseNLog();
                    webBuilder.UseKestrel();
                })
                .Build()
                .Run();
        }

        private static async Task ContinuationAction(Task obj)
        {
            var config = new ProducerConfig
            {
                BootstrapServers = "10.10.36.211:9092"
            };
            var producerBuilder = new ProducerBuilder<string, string>(config);
            
            var producer = producerBuilder.Build();
            
            for (int i = 0; i < 10000000; i++)
            {
                await producer.ProduceAsync("dogac-test-topic", new Message<string, string>
                {
                    Key = "key" + i % 10,
                    Value = "value" + i % 10
                });

                await Task.Delay(200);
            }

            ;
        }
    }
}