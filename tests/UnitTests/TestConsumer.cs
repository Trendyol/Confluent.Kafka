using System;
using System.Threading.Tasks;
using Confluent.Kafka.Lib.Core;

namespace Confluent.Kafka.Lib.Tests.UnitTests
{
    public class TestConsumer : KafkaConsumer<string, string>
    {
        public TestConsumer(string topic, IConsumer<string, string> consumer) : base(topic, consumer)
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