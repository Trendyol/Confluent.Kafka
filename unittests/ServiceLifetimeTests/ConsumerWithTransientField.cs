using System;
using System.Threading.Tasks;
using Confluent.Kafka;
using NUnit.Framework;

namespace Trendyol.Confluent.Kafka.Tests.ServiceLifetimeTests
{
    public class ConsumerWithTransientField : KafkaConsumer
    {
        private readonly TestService _testService;

        public ConsumerWithTransientField(TestService testService)
        {
            _testService = testService;
        }

        protected override Task OnConsume(ConsumeResult<string, string> result)
        {
            _testService.DoWork();
            
            return Task.CompletedTask;
        }

        protected override Task OnError(Exception exception, ConsumeResult<string, string> result)
        {
            Assert.Fail(exception.ToString());

            return Task.CompletedTask;
        }
    }
}