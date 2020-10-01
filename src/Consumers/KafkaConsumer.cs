using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka.Lib.Core.Extensions;
using Microsoft.Extensions.Hosting;
using static Confluent.Kafka.Lib.Core.Global.Constants;

namespace Confluent.Kafka.Lib.Core.Consumers
{
    public abstract class KafkaConsumer : BackgroundService
    {
        private bool _disposed;
        private string? _topic;
        private int _commitPeriod;
        private int _maxRetryCount;
        private CancellationToken _cancellationToken;
        private IConsumer<string, string>? _consumer;
        private IProducer<string, string>? _producer;
        private ConsumerConfig? _consumerConfig;

        /// <summary>
        /// Do not remove, this method gets called via reflection.
        /// This setup is made to support DependencyInjection and
        /// a parameterless constructor for KafkaConsumer
        /// We have to both provide a parameterless constructor for
        /// easier use of clients and do not break encapsulation.
        /// </summary>
        /// <param name="consumerConfig">Consumer config for both main and retry consumers.</param>
        /// <param name="producerConfig">Producer config for producing to retry and failed topics.</param>
        /// <param name="topic">Main topic to subscribe to.</param>
        /// <param name="commitPeriod">Messages are committed in batches of commit period.</param>
        /// <param name="maxRetryCount">Maximum retry count for re-processing messages.</param>
        /// <exception cref="ArgumentNullException"></exception>
        /// <exception cref="ArgumentException"></exception>
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

            if (topic == null)
            {
                throw new ArgumentNullException("Topic cannot be null");
            }

            if (commitPeriod < 1)
            {
                throw new ArgumentException("Commit period should at least be 1.");
            }

            // I should think what if consumer starts to commit consumed messages
            // TODO: May support AutoCommit in the future
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
            _consumerConfig = consumerConfig;
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
            // This can happen if the Host is up and running
            // but SetFields method is not called
            // TODO: We can specify a small wait in order for Host to be up
            if (_consumer == null ||
                _producer == null)
            {
                throw new InvalidOperationException();
            }

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

                            // TODO: Should be configurable
                            var retryTopic = _topic + ".retry";

                            // TODO: Produce can throw
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

                // goto is used to reduce nesting
                // You can look at Linux Developer Guidelines
                // if you are not a fan of goto
                goto TRY_AGAIN;
            }
        }
        
        private async Task StartRetryConsumerLoop()
        {
            if (_consumer == null ||
                _producer == null)
            {
                // This can only happen if initialization takes longer than
                // the specified retry timespan which is very unlikely
                // Added this check to suppress CS8618 warning
                throw new InvalidOperationException();
            }

            do
            {
                // First wait and then consume messages to
                // wait for some failed messages to accumulate in
                // retry topic
                // TODO: Delay time should be configurable
                await Task.Delay(TimeSpan.FromSeconds(3), _cancellationToken);
                
                // Create a new consumer for every retry run, by doing this
                // we don't have to introduce another field like _retryConsumer 
                // Because it is created fairly infrequently
                // I don't think it will be a perf problem
                using var retryConsumer = new ConsumerBuilder<string, string>(_consumerConfig)
                    .Build();

                try
                {
                    // TODO: RetryTopic should be configurable
                    var retryTopic = _topic + ".retry";
                    
                    retryConsumer.Subscribe(retryTopic);

                    while (!_cancellationToken.IsCancellationRequested)
                    {
                        // If we do not get any messages within three seconds
                        // we assume that we are at the end of all committed messages
                        var result = retryConsumer.Consume(TimeSpan.FromSeconds(3));

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
                                // TODO: FailedTopic should be configurable
                                var failedTopic = _topic + ".failed";

                                // Currently, we do not check the result of Produce (DeliveryResult)
                                // Also, produce can throw exceptions and those should be handled
                                // in a "at least once semantics"
                                // TODO: Produce can throw
                                // TODO: We may check DeliveryResult
                                await _producer.ProduceAsync(failedTopic, result.Message, _cancellationToken);
                            }
                            else
                            {
                                await _producer.ProduceAsync(retryTopic, result.Message, _cancellationToken);
                            }
                        }

                        // Commit period is supported to provide high throughput
                        // and reduce latency in consuming messages
                        // If we commit every message after we read
                        if (result.Offset % _commitPeriod == 0)
                        {
                            // We commit everything that has been consumed and processed
                            // Even if we could not process the message, we are
                            // producing the same message to some topic, so
                            // it is safe to commit all here
                            retryConsumer.Commit(result);
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
                    await OnError(e);
                }
                
            } while (!_cancellationToken.IsCancellationRequested);
        }

        /// <summary>
        /// Process your message here.
        /// </summary>
        /// <param name="message">Message that has been processed.</param>
        /// <returns>Task that represents this operation</returns>
        protected abstract Task OnConsume(Message<string, string> message);
        
        /// <summary>
        /// Implementers of this method shouldn't try to implement kafka-level
        /// error handling as it is handled it base consumer.
        /// You can log your exceptions here.
        /// </summary>
        /// <param name="exception"></param>
        /// <returns>Task that represents this operation</returns>
        protected abstract Task OnError(Exception exception);

        public override void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            
            // Call Close instead of Dispose to
            // ensure a timely consumer group re-balance
            _consumer?.Close();
            _producer?.Dispose();
            base.Dispose();
        }
    }
}