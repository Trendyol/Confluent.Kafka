using System;
using System.IO;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using NUnit.Framework;

namespace Confluent.Kafka.Utility.Tests.IntegrationTests.Fakes
{
    public class IntegrationTestBase
    {
        private static IConfiguration Configuration { get; set; }
        
        private IServiceProvider _serviceProvider;

        [OneTimeSetUp]
        public void OneTimeSetUp()
        {
            Configuration = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddEnvironmentVariables()
                .AddJsonFile("appsettings.json")
                .Build();
            
            var webHostBuilder = new WebHostBuilder();
            webHostBuilder.UseStartup<TestStartup>();
            webHostBuilder.UseConfiguration(Configuration);
            
            var testServer = new TestServer(webHostBuilder);
            _serviceProvider = testServer.Host.Services;
        }
        
        protected TType GetRequiredService<TType>()
        {
            return _serviceProvider.GetRequiredService<TType>();
        }
    }
}
