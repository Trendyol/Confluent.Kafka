using System;
using System.Threading.Tasks;
using Confluent.Kafka.Lib.Core.Consumers;
using Microsoft.Extensions.Logging;

namespace Confluent.Kafka.Lib.Example
{
    public class TestConsumer : KafkaConsumer
    {
        private readonly ILogger<TestConsumer> _logger;

        public TestConsumer(ILogger<TestConsumer> logger)
        {
            _logger = logger;
        }

        protected override Task OnConsume(Message<string, string> message)
        {
            _logger.LogInformation($"Key : {message.Key}, Value : {message.Value}");
            
            return Task.CompletedTask;
        }

        protected override Task OnError(Exception exception)
        {
            _logger.LogError(exception.ToString());
            
            return Task.CompletedTask;
        }
    }
}