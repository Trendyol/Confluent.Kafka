using System;
using System.Threading.Tasks;

namespace Confluent.Kafka.Utility.Tests.UnitTests
{
    public class UnitTestConsumer : KafkaConsumer<string, string>
    {
        public UnitTestConsumer(string topic, IConsumer<string, string> consumer) : base(topic, consumer)
        {
        }

        protected override Task ProcessRecord(ConsumeResult<string, string> result)
        {
            return Task.CompletedTask;
        }

        protected override Task OnProcessError(Exception exception, ConsumeResult<string, string> result)
        {
            return Task.CompletedTask;
        }

        protected override Task OnConsumeError(ConsumeException exception)
        {
            return Task.CompletedTask;
        }

        protected override Task OnCommitError(KafkaException exception, ConsumeResult<string, string> result)
        {
            return Task.CompletedTask;
        }
    }
}