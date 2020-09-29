using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka.Lib.Core.Extensions;
using Microsoft.Extensions.Hosting;
using static Confluent.Kafka.Lib.Core.Constants;

namespace Confluent.Kafka.Lib.Core.Consumers
{
    // TODO: Remove code duplication
    public abstract class KafkaConsumer : BackgroundService
    {
        private bool _disposed;
        private string _topic;
        private IConsumer<string, string> _consumer;
        private IConsumer<string, string> _retryConsumer;
        private IProducer<string, string> _producer;
        private int _commitPeriod;
        private int _maxRetryCount;
        private Timer _retryConsumerTimer;
        private CancellationToken _cancellationToken;
        
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
            var retryConsumerBuilder = new ConsumerBuilder<string, string>(consumerConfig);
            var producerBuilder = new ProducerBuilder<string, string>(producerConfig);

            _consumer = consumerBuilder.Build();
            _retryConsumer = retryConsumerBuilder.Build();
            _producer = producerBuilder.Build();
            _topic = topic ?? throw new ArgumentNullException(nameof(topic));
            _maxRetryCount = maxRetryCount;
            _commitPeriod = commitPeriod;
            _retryConsumerTimer = new Timer(async _ => await ConsumeRetryMessages(), null,
                TimeSpan.FromMinutes(15), TimeSpan.FromMinutes(15));
        }

        private async Task ConsumeRetryMessages()
        {
            try
            {
                if (_cancellationToken.IsCancellationRequested)
                {
                    if (_retryConsumer == null)
                    {
                        return;
                    }

                    await _retryConsumerTimer.DisposeAsync();

                    return;
                }

                var retryTopic = _topic + ".retry";
                
                _retryConsumer.Subscribe(retryTopic);

                while (true)
                {
                    var result = _retryConsumer.Consume(TimeSpan.FromSeconds(3));

                    if (result == null)
                    {
                        break;
                    }

                    if (result.Message == null)
                    {
                        continue;
                    }
                
                    result.Message.IncrementHeaderValueAsInt(RetryCountHeaderKey);

                    try
                    {
                        await OnConsume(result.Message);
                    }
                    catch (Exception e)
                    {
                        await OnError(e);
                    
                        var retryCount = result.Message.GetHeaderValue<int>(RetryCountHeaderKey);

                        if (retryCount > _maxRetryCount)
                        {
                            var failedTopic = _topic + ".failed";

                            await _producer.ProduceAsync(failedTopic, result.Message, _cancellationToken);
                        }
                        else
                        {
                            await _producer.ProduceAsync(retryTopic, result.Message, _cancellationToken);
                        }
                    }

                    if (result.Offset % _commitPeriod == 0)
                    {
                        _retryConsumer.Commit(result);
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
            }
        }

        protected override Task ExecuteAsync(CancellationToken token)
        {
            _cancellationToken = token;

            Task.Factory.StartNew(async () => await RunMainConsumer(token),
                token, TaskCreationOptions.LongRunning, TaskScheduler.Default);

            return Task.CompletedTask;
        }

        private async Task RunMainConsumer(CancellationToken token)
        {
            TRY_AGAIN:

            try
            {
                _consumer.Subscribe(_topic);

                while (!token.IsCancellationRequested)
                {
                    try
                    {
                        var result = _consumer.Consume(token);

                        _consumer.Consume(token);

                        if (result.Message == null)
                        {
                            continue;
                        }

                        try
                        {
                            await OnConsume(result.Message);
                        }
                        catch (Exception e)
                        {
                            await OnError(e);

                            var retryTopic = _topic + ".retry";

                            await _producer.ProduceAsync(retryTopic, result.Message, token);
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

                goto TRY_AGAIN;
            }
        }

        protected abstract Task OnConsume(Message<string, string> message);
        protected abstract Task OnError(Exception exception);

        public override void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            _consumer?.Close();
            _retryConsumer?.Close();
            _producer?.Dispose();
            _retryConsumerTimer?.Dispose();
            base.Dispose();
        }
    }
}