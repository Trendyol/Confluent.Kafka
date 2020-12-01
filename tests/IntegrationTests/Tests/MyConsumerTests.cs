using System.Threading;
using System.Threading.Tasks;
using AutoFixture;
using FluentAssertions;
using NUnit.Framework;

namespace Confluent.Kafka.Utility.Tests.IntegrationTests.Tests
{
    public class MyConsumerTests : ConsumerTestBase
    {
        private IFixture _fixture;

        [SetUp]
        public override async Task SetUp()
        {
            await base.SetUp();
            
            _fixture = new Fixture();
        }

        [TearDown]
        public override async Task TearDown()
        {
            await base.TearDown();
        }

        [Test]
        public async Task RunAsync_WhenValidRecordProduced_ShouldConsumeRecord()
        {
            var key = _fixture.Create<string>();
            var value = _fixture.Create<string>();
            var mre = new ManualResetEvent(false);
            
            var processedRecord = null as ConsumeResult<string, string>;

            using var consumer = CreateConsumer<string, string>(Topic);

            await consumer.RunAsync();
            
            await Task.Delay(500);

            consumer.RecordProcessed += result =>
            {
                processedRecord = result;
                mre.Set();
            };

            await ProduceMessageAsync(key, value);

            mre.WaitOne();

            processedRecord.Message.Key.Should().Be(key);
            processedRecord.Message.Value.Should().Be(value);
        }
        
        [Test]
        public async Task RunAsync_WhenDoubleMessageProducedAndStringIsConsumed_ShouldNotGiveConsumeError()
        {
            var key = _fixture.Create<double>();
            var value = _fixture.Create<double>();

            using var consumer = CreateConsumer<string, string>(Topic);
            
            await consumer.RunAsync();

            await Task.Delay(500);

            consumer.RecordProcessed += result =>
            {
                Assert.Pass();
            };
            consumer.OnConsumeErrored += result =>
            {
                Assert.Fail();
            };

            await Producer.ProduceAsync(Topic, new Message<double, double>
            {
                Key = key,
                Value = value
            });
        }
        
        [Test]
        public async Task RunAsync_WhenLongMessageProduced_ShouldConsumeRecord()
        {
            var key = _fixture.Create<long>();
            var value = _fixture.Create<long>();

            using var consumer = CreateConsumer<long, long>(Topic);

            await consumer.RunAsync();

            await Task.Delay(500);
            
            var mre = new ManualResetEvent(false);

            consumer.RecordProcessed += result =>
            {
                result.Should().NotBeNull();
                result.Message.Key.Should().Be(key);
                result.Message.Value.Should().Be(value);
                mre.Set();
            };
            consumer.OnConsumeErrored += result =>
            {
                Assert.Fail();
            };
            consumer.OnCommitErrored += (exception, result) =>
            {
                Assert.Fail();
            };
            consumer.OnProcessErrored += (exception, result) =>
            {
                Assert.Fail();
            };

            await ProduceMessageAsync(key, value);

            mre.WaitOne();
        }
        
        [Test]
        public async Task RunAsync_WhenCancellationTokenGiven_ShouldEndWhenTokenCancelled()
        {
            var key = _fixture.Create<string>();
            var value = _fixture.Create<string>();
            using var consumer = CreateConsumer<long, long>(Topic);
            
            var cts = new CancellationTokenSource();

            await consumer.RunAsync(cts.Token);

            await Task.Delay(500, cts.Token);

            consumer.RecordProcessed += result =>
            {
                Assert.Fail();
            };
            consumer.OnConsumeErrored += result =>
            {
                Assert.Fail();
            };
            consumer.OnCommitErrored += (exception, result) =>
            {
                Assert.Fail();
            };
            consumer.OnProcessErrored += (exception, result) =>
            {
                Assert.Fail();
            };
            
            cts.Cancel();

            for (var i = 0; i < 10; i++)
            {
                await ProduceMessageAsync(key, value);
            }

            // ReSharper disable once MethodSupportsCancellation
            await Task.Delay(1000);

            Assert.Pass();
        }
    }
}