using System;
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
                        })
                        .ConfigureAppConfiguration(ConfigureApplication);

                    webBuilder.UseConfiguration(configuration);
                    webBuilder.UseStartup<Startup>();
                    webBuilder.UseNLog();
                    webBuilder.UseKestrel();
                })
                .Build()
                .Run();
        }

        private static void ConfigureApplication(
            WebHostBuilderContext context,
            IConfigurationBuilder builder)
        {
            string environment = context
                .HostingEnvironment
                .EnvironmentName
                .ToLowerInvariant();

            if (environment == "development")
            {
                return;
            }

            string environmentFileName = $"nlog.{environment}.config";

            if (!File.Exists(environmentFileName))
            {
                return;
            }

            if (File.Exists("nlog.config"))
            {
                File.Delete("nlog.config");
            }

            File.Move(environmentFileName, "nlog.config");
        }
    }
}