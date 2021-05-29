using System;

namespace Trendyol.Confluent.Kafka.HostedServiceTests.Services
{
    public class Service : IService
    {
        public void WriteToConsole(string arg)
        {
            Console.WriteLine(arg);
        }
    }
}