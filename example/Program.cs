using System.IO;
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
    }
}