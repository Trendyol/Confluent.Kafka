using System;
using System.IO;
using System.Security.Cryptography.Xml;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using NUnit.Framework;

namespace Trendyol.Confluent.Kafka.Tests.Base
{
    public abstract class IntegrationTestBase
    {
        private IServiceProvider _serviceProvider;
        private StringWriter _stdOut = new StringWriter();

        [OneTimeSetUp]
        public virtual Task OneTimeSetUp()
        {
            var webHostBuilder = new WebHostBuilder();
            webHostBuilder.UseStartup<TestStartup>();

            var testServer = new TestServer(webHostBuilder);
            _serviceProvider = testServer.Host.Services;

            Console.SetOut(_stdOut);

            return Task.CompletedTask;
        }

        protected T GetRequiredService<T>()
        {
            return _serviceProvider.GetRequiredService<T>();
        }

        protected string GetLogs()
        {
            return _stdOut.ToString();
        }
    }
}