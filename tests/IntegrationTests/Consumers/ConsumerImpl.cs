using System;
using System.Threading.Tasks;
using Confluent.Kafka.Lib.Core;

namespace Confluent.Kafka.Lib.Tests.IntegrationTests.Consumers
{
    public class ConsumerImpl<TKey, TValue> : KafkaConsumer<TKey, TValue>, IConsumerImpl
    {
        public event Action<ConsumeResult<TKey, TValue>> RecordProcessed = delegate {  };
        public event Action<Exception, ConsumeResult<TKey, TValue>> OnProcessErrored = delegate {  };
        public event Action<ConsumeException> OnConsumeErrored = delegate {  };
        public event Action<KafkaException, ConsumeResult<TKey, TValue>> OnCommitErrored = delegate {  };
        
        public ConsumerImpl(string topic, IConsumer<TKey, TValue> consumer) : base(topic, consumer)
        {
        }

        protected override Task ProcessRecord(ConsumeResult<TKey, TValue> result)
        {
            RecordProcessed.Invoke(result);
            
            return Task.CompletedTask;
        }

        protected override Task OnProcessError(Exception exception, ConsumeResult<TKey, TValue> result)
        {
            OnProcessErrored.Invoke(exception, result);
            
            return Task.CompletedTask;
        }

        protected override Task OnConsumeError(ConsumeException exception)
        {
            OnConsumeErrored.Invoke(exception);
            
            return Task.CompletedTask;
        }

        protected override Task OnCommitError(KafkaException exception, ConsumeResult<TKey, TValue> result)
        {
            OnCommitErrored.Invoke(exception, result);
            
            return Task.CompletedTask;
        }
    }
}