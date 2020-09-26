using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka.Lib.Core.Extensions;
using Microsoft.Extensions.Logging;

namespace Confluent.Kafka.Lib.Core.Consumers
{
    public abstract class KafkaConsumer : IKafkaConsumer, IDisposable
    {
        private bool _disposed;
        private readonly string _topic;
        private readonly IConsumer<string, string> _consumer;
        private readonly IProducer<string, string> _producer;
        private readonly ILogger<KafkaConsumer> _logger;
        private readonly int _commitPeriod;

        public KafkaConsumer(ConsumerConfig consumerConfig,
            ProducerConfig errorProducerConfig,
            string topic,
            int commitPeriod,
            ILogger<KafkaConsumer> logger)
        {
            if (consumerConfig == null || errorProducerConfig == null)
            {
                throw new ArgumentNullException("Config values cannot be null.");
            }
            if (commitPeriod < 1)
            {
                throw new ArgumentException("Commit period should at least be 1.");
            }

            var consumerBuilder = new ConsumerBuilder<string, string>(consumerConfig);
            var producerBuilder = new ProducerBuilder<string, string>(errorProducerConfig);

            _consumer = consumerBuilder.Build();
            _producer = producerBuilder.Build();
            _topic = topic ?? throw new ArgumentNullException(nameof(topic));
            _logger = logger ?? throw new ArgumentNullException(nameof(Logger<KafkaConsumer>));
            _commitPeriod = commitPeriod;
        }

        public async Task Run(CancellationToken token, ConsumerType consumerType)
        {
            try
            {
                var success = await Try(() => { _consumer.Subscribe(_topic); }, 5, token);

                if (!success)
                {
                    _logger.LogError($"Consumer : {GetType().Name} is down because of fatal error.");
                    return;
                }

                var period = _commitPeriod;

                while (!token.IsCancellationRequested)
                {
                    ConsumeResult<string, string> result = null;
                    
                    try
                    {
                        result = _consumer.Consume(token);
                    }
                    catch (KafkaException e)
                    {
                        _logger.LogError(e.ToString());
                        
                        if (e.Error.Code == ErrorCode.Local_UnknownTopic ||
                            e.Error.Code == ErrorCode.Local_UnknownPartition ||
                            e.Error.IsFatal)
                        {
                            _logger.LogError($"Consumer : {GetType().Name} is ending because of fatal error.");
                            return;
                        }
                        
                        continue;
                    }
                    
                    if (result?.Message == null)
                    {
                        continue;
                    }

                    try
                    {
                        if (consumerType == ConsumerType.Retry)
                        {
                            var message = result.Message;

                            message.IncrementHeaderValueAsInt(Constants.RetryCount);
                        }
                        
                        await OnConsume(result);
                    }
                    catch (Exception e)
                    {
                        _logger.LogError(e.ToString());
                        
                        var message = result.Message;

                        var retryCount = message.GetHeaderValue<int>(Constants.RetryCount);

                        if (consumerType == ConsumerType.Retry)
                        {
                            if (retryCount > Constants.MaxRetryValue)
                            {
                                var failedTopic = _topic + ".FAILED";

                                await _producer.ProduceAsync(failedTopic, message, token); // TODO: Produce can throw
                            }
                            else
                            {
                                var errorTopic = _topic + ".ERROR";

                                await _producer.ProduceAsync(errorTopic, message, token);
                            }
                        }
                        else
                        {
                            var errorTopic = _topic + ".ERROR";

                            await _producer.ProduceAsync(errorTopic, message, token);
                        }
                    }

                    --period;

                    if (period == 0)
                    {
                        try
                        {
                            _consumer.Commit();
                        }
                        catch (Exception e)
                        {
                            _logger.LogError(e.ToString());
                            _logger.LogError($"Consumer : {GetType().Name} is down because of commit error.");
                            return;
                        }

                        period = _commitPeriod;
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
        }

        private async Task<bool> Try(Action action,
            int times,
            CancellationToken token)
        {
            var millisecondsDelay = 50;

            RETRY:

            if (token.IsCancellationRequested)
            {
                return false;
            }

            try
            {
                action();

                return true;
            }
            catch (Exception e)
            {
                _logger.LogError(e.ToString());

                await Task.Delay(millisecondsDelay, token);

                millisecondsDelay *= 2;
                times--;

                if (times > 0)
                {
                    goto RETRY;
                }

                return false;
            }
        }

        protected abstract Task OnConsume(ConsumeResult<string, string> consumeResult);

        #region DISPOSE_PATTERN

        protected virtual void Dispose(bool disposing)
        {
            if (_disposed)
            {
                return;
            }

            if (disposing)
            {
                _consumer?.Close();
            }

            _disposed = true;
        }

        public void Dispose()
        {
            Dispose(true);
        }

        #endregion
    }
}