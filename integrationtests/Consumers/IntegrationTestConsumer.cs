using System;
using System.Threading.Tasks;
using Confluent.Kafka;
using Trendyol.Confluent.Kafka.Tests.Helpers;
using Trendyol.Confluent.Kafka.Tests.Tests;

namespace Trendyol.Confluent.Kafka.Tests.Consumers
{
    public sealed class IntegrationTestConsumer : KafkaConsumer
    {
        protected override Task OnConsume(ConsumeResult<string, string> result)
        {
            var log = LogHelper.CreateEventConsumedLog(result.Message.Key, result.Message.Value);

            Console.WriteLine(log);

            ConsumerCommunicator.InformMessageConsumed();

            return Task.CompletedTask;
        }

        protected override Task OnError(Exception exception, ConsumeResult<string, string>? result)
        {
            var log = LogHelper.CreateAnErrorOccurredLog(exception, result?.Message?.Key, result?.Message?.Value);

            Console.WriteLine(log);

            return Task.CompletedTask;
        }
    }
}