using System;
using System.Threading.Tasks;
using Confluent.Kafka.Lib.Core.Consumers;
using Microsoft.AspNetCore.Components.RenderTree;
using Microsoft.Extensions.Logging;

namespace Confluent.Kafka.Lib.Example
{
    public class OmsConsumer : KafkaConsumer
    {
        private readonly ILogger<OmsConsumer> _logger;

        public OmsConsumer(ILogger<OmsConsumer> logger)
        {
            _logger = logger;
        }

        protected override Task OnConsume(ConsumeResult<string, string> consumeResult)
        {
            var key = consumeResult.Message.Key;
            var value = consumeResult.Message.Value;
            _logger.LogInformation($"Key : {key}, Value : {value}");
            
            return Task.CompletedTask;
        }

        protected override Task OnError(Exception exception)
        {
            _logger.LogError(exception.ToString());
            
            return Task.CompletedTask;
        }
    }
}