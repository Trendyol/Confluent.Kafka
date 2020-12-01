using System;

namespace Confluent.Kafka.Utility.Tests.IntegrationTests.Consumers
{
    public interface IConsumerImpl : IKafkaConsumer, IDisposable
    {
    }
}