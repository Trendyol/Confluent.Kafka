using System;
using System.Threading.Tasks;
using Confluent.Kafka.Lib.Core.Consumers;
using Microsoft.Extensions.Logging;

namespace Confluent.Kafka.Lib.Core
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
            throw new Exception(consumeResult.Message.Value);
        }

        protected override Task OnError(Exception exception)
        {
            _logger.LogError(exception.ToString());
            
            return Task.CompletedTask;
        }
    }
}