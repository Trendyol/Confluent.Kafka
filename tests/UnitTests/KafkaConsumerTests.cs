using System.Threading;
using AutoFixture;
using FluentAssertions;
using Moq;
using NUnit.Framework;

namespace Confluent.Kafka.Utility.Tests.UnitTests
{
    public class KafkaConsumerTests
    {
        private KafkaConsumer<string, string> _sut;
        private IFixture _fixture;
        private string _topic;
        private Mock<IConsumer<string, string>> _mockConsumer;

        [SetUp]
        public void SetUp()
        {
            _fixture = new Fixture();
            _topic = _fixture.Create<string>();
            _mockConsumer = new Mock<IConsumer<string, string>>();

            _sut = new TestConsumer(_topic, _mockConsumer.Object);
        }

        [Test]
        public void RunAsync_ShouldRun_WithNoCancellationToken()
        {
            var task = _sut.RunAsync(CancellationToken.None);

            task.IsCompleted.Should().Be(true);
        }
        
        [Test]
        public void RunAsync_ShouldRun_WithDefaultCancellationToken()
        {
            var task = _sut.RunAsync(new CancellationToken());

            task.IsCompleted.Should().Be(true);
        }
        
        [Test]
        public void RunAsync_ShouldRun_WithNotCanceledCancellationToken()
        {
            var task = _sut.RunAsync(new CancellationToken(false));

            task.IsCompleted.Should().Be(true);
        }
    }
}