using System;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace Trendyol.Confluent.Kafka.Tests
{
    public class TestConsumer : KafkaConsumer
    {
        public TestConsumer()
        {
        }

        public TestConsumer(KafkaConfiguration configuration) : base(configuration)
        {
        }
        
        public event Action<ConsumeResult<string, string>> OnConsumeEvent = delegate {  }; 
        public event Action<Exception, ConsumeResult<string, string>> OnErrorEvent = delegate {  };
        
        protected override Task OnConsume(ConsumeResult<string, string> result)
        {
            OnConsumeEvent.Invoke(result);
            
            return Task.CompletedTask;
        }

        protected override Task OnError(Exception exception, ConsumeResult<string, string> result)
        {
            OnErrorEvent.Invoke(exception, result);
            
            return Task.CompletedTask;
        }
    }
}