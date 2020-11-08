using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka.Lib.Core.Configuration;
using Confluent.Kafka.Lib.Core.Extensions;
using Confluent.Kafka.Lib.Core.Serialization;
using Microsoft.Extensions.Hosting;
using static Confluent.Kafka.Lib.Core.Global.Constants;

namespace Confluent.Kafka.Lib.Core.Consumers
{
    public abstract class KafkaConsumer<TKey, TValue> : BackgroundService
    {
        private KafkaConfig? _config;
        private CancellationToken _cancellationToken;
        private GenericSerializer<TKey> _keySerializer = new GenericSerializer<TKey>();
        private GenericSerializer<TValue> _valueSerializer = new GenericSerializer<TValue>();
        private GenericDeserializer<TKey> _keyDeserializer = new GenericDeserializer<TKey>();
        private GenericDeserializer<TValue> _valueDeserializer = new GenericDeserializer<TValue>();

        /// <summary>
        /// Do not remove, this method gets called via reflection.
        /// This setup is made to support dependency injection and
        /// a parameterless constructor for KafkaConsumer.
        /// We have to both provide a parameterless constructor and a private
        /// set method for easier use of clients and do not break encapsulation.
        /// </summary>
        private void SetConfiguration(KafkaConfig config)
        {
            if (config == null)
            {
                throw new ArgumentNullException("KafkaConfig cannot be null.");
            }

            if (config.MainConsumerConfig == null ||
                config.RetryConsumerConfig == null ||
                config.RetryProducerConfig == null)
            {
                throw new ArgumentException("Consumer and producer configs cannot be null.");
            }

            if (config.Topic == null ||
                config.RetryTopic == null ||
                config.FailedTopic == null)
            {
                throw new ArgumentException("Topics cannot be null.");
            }

            if ((config.MainConsumerConfig.IsAutoCommitEnabled() ||
                 config.RetryConsumerConfig.IsAutoCommitEnabled()) &&
                config.CommitPeriod != null)
            {
                throw new ArgumentException("Both manual commit with commit period and auto commit cannot be set.");
            }

            if (config.MaxRetryCount == null)
            {
                throw new ArgumentException("MaxRetryCount property should be set.");
            }

            if (config.MaxRetryCount.Value < 0)
            {
                throw new ArgumentException("MaxRetryCount must be positive.");
            }

            if ((!config.MainConsumerConfig.IsAutoCommitEnabled() ||
                 !config.RetryConsumerConfig.IsAutoCommitEnabled()) &&
                config.CommitPeriod == null)
            {
                throw new ArgumentException("You should either enable auto commit or set commit period.");
            }

            if ((!config.MainConsumerConfig.IsAutoCommitEnabled() &&
                 config.RetryConsumerConfig.IsAutoCommitEnabled()) ||
                (config.MainConsumerConfig.IsAutoCommitEnabled() &&
                 !config.RetryConsumerConfig.IsAutoCommitEnabled()))
            {
                throw new ArgumentException("If you are using auto-commit, you should use it for both consumers.");
            }

            if ((!config.MainConsumerConfig.IsAutoCommitEnabled() ||
                 !config.RetryConsumerConfig.IsAutoCommitEnabled()) &&
                config.CommitPeriod < 1)
            {
                throw new ArgumentException("Commit period should be at least 1 in manual commits.");
            }

            if (config.RetryPeriod == null)
            {
                throw new AggregateException("RetryPeriod is required.");
            }

            _config = config;
            _ = StartRetryConsumerLoop();
        }

        protected override Task ExecuteAsync(CancellationToken token)
        {
            _cancellationToken = token;

            Task.Factory.StartNew(async () => await StartMainConsumerLoop(token),
                token, TaskCreationOptions.LongRunning, TaskScheduler.Default);

            return Task.CompletedTask;
        }

        private async Task StartMainConsumerLoop(CancellationToken token)
        {
            if (_config == null)
            {
                // Technically config can never be null here because
                // AddKafkaConsumer will always call SetConfiguration method
                // before the HostedService is up
                throw new InvalidOperationException("Config cannot be null.");
            }
            
            using var consumer = new ConsumerBuilder<TKey, TValue>(_config.MainConsumerConfig)
                .SetKeyDeserializer(_keyDeserializer)
                .SetValueDeserializer(_valueDeserializer)
                .Build();
            using var producer = new ProducerBuilder<TKey, TValue>(_config.RetryProducerConfig)
                .SetKeySerializer(_keySerializer)
                .SetValueSerializer(_valueSerializer)
                .Build();

            while (true)
            {
                try
                {
                    consumer.Subscribe(_config.Topic);

                    while (!token.IsCancellationRequested)
                    {
                        try
                        {
                            var result = consumer.Consume(token);

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
                                await OnError(e, result.Message);

                                // TODO: Produce can throw
                                await producer.ProduceAsync(_config.RetryTopic, result.Message, token);
                            }

                            if (_config.CommitPeriod != null)
                            {
                                if (result.Offset % _config.CommitPeriod == 0)
                                {
                                    try
                                    {
                                        consumer.Commit(result);
                                    }
                                    catch (KafkaException e)
                                    {
                                        await OnError(e, result.Message);
                                    }
                                }
                            }
                        }
                        catch (ConsumeException e)
                        {
                            await OnError(e, null);
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (ObjectDisposedException)
                {
                    break;
                }
                catch (AccessViolationException)
                {
                    break;
                }
                catch (Exception e)
                {
                    await OnError(e, null);

                    await Task.Delay(50, token);
                }
            }
        }

        private async Task StartRetryConsumerLoop()
        {
            // We just set config before calling this method
            // therefore it can never be null
            if (_config == null)
            {
                throw new InvalidOperationException("Config cannot be null.");
            }
            
            // Create a new consumer and producer locally, by doing this
            // we don't have to introduce another field like _retryConsumer
            using var consumer = new ConsumerBuilder<TKey, TValue>(_config.RetryConsumerConfig)
                .SetKeyDeserializer(_keyDeserializer)
                .SetValueDeserializer(_valueDeserializer)
                .Build();
            using var producer = new ProducerBuilder<TKey, TValue>(_config.RetryProducerConfig)
                .SetKeySerializer(_keySerializer)
                .SetValueSerializer(_valueSerializer)
                .Build();

            do
            {
                // First wait and then consume messages to
                // wait for some failed messages to accumulate in
                // retry topic
                await Task.Delay(_config.RetryPeriod!.Value, _cancellationToken);

                try
                {
                    consumer.Subscribe(_config.RetryTopic);

                    while (!_cancellationToken.IsCancellationRequested)
                    {
                        // If we do not get any messages within three seconds
                        // we assume that we are at the end of all committed messages
                        var result = consumer.Consume(TimeSpan.FromSeconds(3));

                        // ConsumeResult returns null if we can't get
                        // any messages within specified timeout
                        if (result == null)
                        {
                            break;
                        }

                        // We never process or re-process null messages
                        // Because we are not processing them in main consumer
                        // this shouldn't be a case, but we'd better check it
                        if (result.Message == null)
                        {
                            continue;
                        }

                        // Increment before we process the message
                        // because we are always going to try to process the message
                        // whether it is processed or not
                        result.Message.IncrementHeaderValue(RetryCountHeaderKey);

                        try
                        {
                            await OnConsume(result.Message);
                        }
                        catch (Exception e)
                        {
                            await OnError(e, result.Message);

                            var retryCount = result.Message.GetHeaderValue(RetryCountHeaderKey);

                            if (retryCount > _config.MaxRetryCount)
                            {
                                // Currently, we do not check the result of Produce (DeliveryResult)
                                // Also, produce can throw exceptions and those should be handled
                                // in a "at least once semantics"
                                // TODO: Produce can throw
                                // TODO: We may check DeliveryResult
                                await producer.ProduceAsync(_config.FailedTopic, result.Message, _cancellationToken);
                            }
                            else
                            {
                                await producer.ProduceAsync(_config.RetryTopic, result.Message, _cancellationToken);
                            }
                        }

                        if (_config.CommitPeriod != null)
                        {
                            // Commit period is supported to provide high throughput
                            // and reduce latency in consuming messages
                            // If we commit every message after we read
                            if (result.Offset % _config.CommitPeriod == 0)
                            {
                                // We commit everything that has been consumed and processed
                                // Even if we could not process the message, we are
                                // producing the same message to some topic, so
                                // it is safe to commit all here
                                consumer.Commit(result);
                            }
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                }
                catch (ObjectDisposedException)
                {
                }
                // This can be sometimes called instead of ObjectDisposedException
                // there are some issues that are open concerning this issue
                // this can get thrown if the host is down but consumer is
                // still trying to consume messages from a TopicPartition
                catch (AccessViolationException)
                {
                }
                catch (Exception e)
                {
                    await OnError(e, null);
                }
            } while (!_cancellationToken.IsCancellationRequested);
        }

        /// <summary>
        /// Process your message here.
        /// </summary>
        /// <param name="message">Message that has been processed.</param>
        /// <returns>Task that represents this operation</returns>
        protected abstract Task OnConsume(Message<TKey, TValue> message);

        /// <summary>
        /// Implementers of this method shouldn't try to implement kafka-level
        /// error handling as it is handled it base consumer.
        /// You can log your exceptions here.
        /// </summary>
        /// <param name="exception"></param>
        /// <returns>Task that represents this operation</returns>
        protected abstract Task OnError(Exception exception, Message<TKey, TValue>? message);
    }
}