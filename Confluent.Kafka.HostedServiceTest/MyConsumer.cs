using System;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Utility;
using Microsoft.Extensions.Configuration;

namespace TestApplication
{
    public class MyConsumer : KafkaConsumer
    {
        private readonly IService _service;
        private readonly IConfiguration _configuration;

        public MyConsumer(IService service, 
            IConfiguration configuration)
        {
            _service = service;
            _configuration = configuration;
        }

        protected override Task OnConsume(ConsumeResult<string, string> result)
        {
            var resultMessage = result.Message;
            var output = $"Key : {resultMessage.Key}, Value : {resultMessage.Value}";
            
            _service.WriteToConsole(_configuration.GetValue<string>("BootstrapServers"));
            _service.WriteToConsole(output);
            
            return Task.CompletedTask;
        }

        protected override Task OnError(Exception exception, ConsumeResult<string, string>? result)
        {
            _service.WriteToConsole(exception.ToString());
            _service.WriteToConsole(result?.ToString() ?? "consumeResult : null");
            
            return Task.CompletedTask;
        }
    }
}