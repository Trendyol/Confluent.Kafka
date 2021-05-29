using System;
using FluentAssertions;
using NUnit.Framework;

namespace Trendyol.Confluent.Kafka.Tests.ValidationTests
{
    public class DefaultCtorTests
    {
        [Test]
        public void ShouldCreateTestConsumer()
        {
            _ = new TestConsumer();
        }

        [Test]
        public void WhenConsumerIsNotInitialized_ShouldThrowInvalidOperationException()
        {
            var consumer = new TestConsumer();

            _ = Assert.ThrowsAsync<InvalidOperationException>(async () =>
            {
                await consumer.RunAsync();
            });
        }
    }
}