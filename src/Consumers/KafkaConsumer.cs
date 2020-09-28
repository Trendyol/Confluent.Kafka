using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka.Lib.Core.Extensions;
using Microsoft.Extensions.Hosting;
using static Confluent.Kafka.Lib.Core.Constants;

namespace Confluent.Kafka.Lib.Core.Consumers
{
    public abstract class KafkaConsumer : BackgroundService
    {
        private bool _disposed;
        private string _topic;
        private IConsumer<string, string> _consumer;
        private IProducer<string, string> _producer;
        private int _commitPeriod;
        private int _maxRetryCount;

        private void SetFields(ConsumerConfig consumerConfig,
            ProducerConfig producerConfig,
            string topic,
            int commitPeriod,
            int maxRetryCount)
        {
            if (consumerConfig == null || producerConfig == null)
            {
                throw new ArgumentNullException("Config values cannot be null.");
            }

            if (commitPeriod < 1)
            {
                throw new ArgumentException("Commit period should at least be 1.");
            }

            if (consumerConfig.EnableAutoCommit == true)
            {
                throw new ArgumentException(
                    "Auto commit is not supported due to possible loss of messages during auto commit.");
            }

            if (maxRetryCount < 1)
            {
                throw new ArgumentException("Commit period should at least be 1.");
            }

            var consumerBuilder = new ConsumerBuilder<string, string>(consumerConfig);
            var producerBuilder = new ProducerBuilder<string, string>(producerConfig);

            _consumer = consumerBuilder.Build();
            _producer = producerBuilder.Build();
            _topic = topic ?? throw new ArgumentNullException(nameof(topic));
            _maxRetryCount = maxRetryCount;
            _commitPeriod = commitPeriod;
        }

        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            Task.Factory.StartNew(async () => await Run(stoppingToken),
                stoppingToken, TaskCreationOptions.LongRunning, TaskScheduler.Default);

            return Task.CompletedTask;
        }

        public async Task Run(CancellationToken token)
        {
            RETRY:

            try
            {
                _consumer.Subscribe(_topic);

                while (!token.IsCancellationRequested)
                {
                    try
                    {
                        var result = _consumer.Consume(token);

                        if (result?.Message == null)
                        {
                            continue;
                        }

                        result.Message.IncrementHeaderValueAsInt(RetryCountHeaderKey);

                        try
                        {
                            await OnConsume(result);
                        }
                        catch (Exception e)
                        {
                            await OnError(e);
                            
                            var currentRetryCount = result.Message.GetHeaderValue<int>(RetryCountHeaderKey);

                            if (currentRetryCount > _maxRetryCount)
                            {
                                var errorTopic = _topic + ".error";

                                await _producer.ProduceAsync(errorTopic, result.Message, token);
                            }
                            else
                            {
#pragma warning disable 4014 // Don't await, it will fire after 15 minutes
                                Task.Delay(TimeSpan.FromMinutes(15), token)
                                    .ContinueWith(_ => _producer.Produce(_topic, result.Message), token);
#pragma warning restore 4014
                            }
                        }

                        if (result.Offset % _commitPeriod == 0)
                        {
                            try
                            {
                                _consumer.Commit(result);
                            }
                            catch (KafkaException e)
                            {
                                await OnError(e);
                            }
                        }
                    }
                    catch (ConsumeException e)
                    {
                        await OnError(e);
                    }
                }
            }
            catch (OperationCanceledException)
            {
            }
            catch (ObjectDisposedException)
            {
            }
            catch (AccessViolationException)
            {
            }
            catch (Exception e)
            {
                await OnError(e);

                await Task.Delay(50, token);

                goto RETRY;
            }
        }

        protected abstract Task OnConsume(ConsumeResult<string, string> consumeResult);
        protected abstract Task OnError(Exception exception);

        public override void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            _consumer?.Close();
            _producer?.Dispose();
            base.Dispose();
        }
    }
}