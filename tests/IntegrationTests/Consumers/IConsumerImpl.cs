using System;
using Confluent.Kafka.Lib.Core;

namespace Confluent.Kafka.Lib.Tests.IntegrationTests.Consumers
{
    public interface IConsumerImpl : IKafkaConsumer, IDisposable
    {
    }
}