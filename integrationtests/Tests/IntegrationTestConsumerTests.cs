using System;
using System.Threading.Tasks;
using FluentAssertions;
using NUnit.Framework;
using Trendyol.Confluent.Kafka.Tests.Base;
using Trendyol.Confluent.Kafka.Tests.Helpers;

namespace Trendyol.Confluent.Kafka.Tests.Tests
{
    public class IntegrationTestConsumerTests : ConsumerTestBase
    {
        [Test]
        public async Task ShouldConsumeEvent()
        {
            var key = Guid.NewGuid().ToString();
            var value = Guid.NewGuid().ToString();

            await Produce(Constants.TestTopic, key, value);

            var timeout = TimeSpan.FromSeconds(5);

            ConsumerCommunicator.WaitForMessageToBeConsumed(timeout);

            var logs = GetLogs();
            var eventConsumedLog = LogHelper.CreateEventConsumedLog(key, value);
            logs.Should().Contain(eventConsumedLog);
            logs.Should().NotContain(LogHelper.AnErrorOccurredLog);
        }
    }
}