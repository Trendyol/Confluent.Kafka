using System;
using System.Threading.Tasks;
using Confluent.Kafka;
using Trendyol.Confluent.Kafka.HostedServiceTests.Services;

namespace Trendyol.Confluent.Kafka.HostedServiceTests
{
    public class MyConsumer : KafkaConsumer
    {
        private readonly IService _service;

        public MyConsumer(IService service)
        {
            _service = service;
        }

        protected override Task OnConsume(ConsumeResult<string, string> result)
        {
            var resultMessage = result.Message;
            var output = $"Key : {resultMessage.Key}, Value : {resultMessage.Value}";
            
            _service.WriteToConsole(output);
            
            return Task.CompletedTask;
        }

        protected override Task OnError(Exception exception, ConsumeResult<string, string> result)
        {
            _service.WriteToConsole(exception.ToString());
            _service.WriteToConsole(result?.ToString() ?? "consumeResult : null");
            
            return Task.CompletedTask;
        }
    }
}