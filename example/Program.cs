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
        public static IConfiguration Configuration { get; set; }
        
        public static void Main(string[] args)
        {
            IConfigurationBuilder builder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json");

            Configuration = builder.Build();

            Host.CreateDefaultBuilder(args)
                .ConfigureWebHostDefaults(webBuilder =>
                {
                    webBuilder.ConfigureLogging(l =>
                        {
                            l.ClearProviders();
                            l.AddConsole();
                            l.SetMinimumLevel(LogLevel.Information);
                        })
                        .ConfigureAppConfiguration(
                            (context, builder) =>
                            {
                                string environment = context.HostingEnvironment?.EnvironmentName?.ToLowerInvariant();
                                if (!string.IsNullOrWhiteSpace(environment))
                                {
                                    if (environment == "development")
                                        return;

                                    string environmentFileName = $"nlog.{environment}.config";

                                    if (!File.Exists(environmentFileName))
                                        return;

                                    if (File.Exists("nlog.config"))
                                    {
                                        File.Delete("nlog.config");
                                    }

                                    File.Move(environmentFileName, "nlog.config");
                                }
                            });

                    webBuilder.UseConfiguration(Configuration);
                    webBuilder.UseStartup<Startup>();
                    webBuilder.UseNLog();
                    webBuilder.UseKestrel();
                })
                .Build()
                .Run();
        }
    }
}