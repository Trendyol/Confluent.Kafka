using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using static Trendyol.Confluent.Kafka.ErrorConstants;

namespace Trendyol.Confluent.Kafka
{
    public abstract class KafkaConsumer : IKafkaConsumer
    {
        protected IConsumer<string, string> Consumer;
        private bool _initialized;
        private KafkaConsumerConfig _consumerConfig;
        private bool _disposed;

        public KafkaConsumer()
        {
        }

        public KafkaConsumer(KafkaConsumerConfig consumerConfig)
        {
            Initialize(consumerConfig);
        }

        public void Initialize(KafkaConsumerConfig consumerConfig)
        {
            if (_initialized)
                throw new InvalidOperationException(KafkaConsumerIsAlreadyInitializedMessage);

            _consumerConfig = consumerConfig;

            ValidateConfiguration(_consumerConfig);

            Consumer = BuildConsumer(consumerConfig);

            _initialized = true;
        }

        public Task RunAsync(CancellationToken cancellationToken = default)
        {
            if (!_initialized)
                throw new InvalidOperationException(KafkaConsumerIsNotInitializedMessage);

            Task.Factory.StartNew(async () =>
                {
                    // KafkaConsumer is initialized and _configuration cannot be null
                    await StartConsumeLoop(_consumerConfig!, cancellationToken);
                },
                cancellationToken, TaskCreationOptions.LongRunning, TaskScheduler.Default);

            return Task.CompletedTask;
        }

        private async Task StartConsumeLoop(KafkaConsumerConfig kafkaConsumerConfig, CancellationToken token)
        {
            // KafkaConsumer is initialized and Consumer cannot be null
            // Topics is validated, and cannot be null, or cannot contain null topic
            Consumer.Subscribe(kafkaConsumerConfig.Topics);

            try
            {
                while (!token.IsCancellationRequested)
                {
                    try
                    {
                        var result = null as ConsumeResult<string, string>;

                        try
                        {
                            result = Consumer.Consume(token);
                        }
                        catch (ConsumeException e)
                        {
                            await OnError(e, result);
                            continue;
                        }

                        if (result.Message == null)
                            continue;

                        try
                        {
                            await OnConsume(result);
                        }
                        catch (Exception e)
                        {
                            await OnError(e, result);
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
                    catch (BadImageFormatException)
                    {
                        break;
                    }
                    catch (Exception e)
                    {
                        await LogError(e);
                    }
                }
            }
            finally
            {
                Consumer.Close();
                _disposed = true;
            }
        }

        private async Task LogError(Exception e)
        {
            if (_consumerConfig.ErrorHandler != null)
            {
                _consumerConfig.ErrorHandler(Consumer, new Error(ErrorCode.Local_Application, e.ToString()));
            }
            else
            {
                await Console.Error.WriteLineAsync(e.ToString());
            }
        }

        private void ValidateConfiguration(KafkaConsumerConfig consumerConfig)
        {
            if (consumerConfig == null)
                throw new ArgumentNullException(nameof(KafkaConsumerConfig));

            var topics = consumerConfig.Topics;

            if (topics == null)
                throw new ArgumentNullException(nameof(KafkaConsumerConfig.Topics));

            foreach (var topic in topics)
                if (topic == null)
                    throw new ArgumentNullException(nameof(topic));
        }

        private IConsumer<string, string> BuildConsumer(KafkaConsumerConfig consumerConfig)
        {
            var cfg = consumerConfig as ConsumerConfig;

            return new ConsumerBuilder<string, string>(cfg)
                .SetErrorHandler(consumerConfig.ErrorHandler)
                .SetKeyDeserializer(consumerConfig.KeyDeserializer)
                .SetValueDeserializer(consumerConfig.ValueDeserializer)
                .SetLogHandler(consumerConfig.LogHandler)
                .SetStatisticsHandler(consumerConfig.StatisticsHandler)
                .SetOffsetsCommittedHandler(consumerConfig.OffsetsCommittedHandler)
                .SetPartitionsAssignedHandler(consumerConfig.PartitionsAssignedHandler)
                .SetPartitionsRevokedHandler(consumerConfig.PartitionsRevokedHandler)
                .Build();
        }

        protected abstract Task OnConsume(ConsumeResult<string, string> result);
        protected abstract Task OnError(Exception exception, ConsumeResult<string, string>? result);

        public virtual void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            Consumer?.Close();
            _disposed = true;
        }
    }
}