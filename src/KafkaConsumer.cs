using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using static Trendyol.Confluent.Kafka.ErrorConstants;

namespace Trendyol.Confluent.Kafka
{
    public abstract class KafkaConsumer : IKafkaConsumer
    {
        protected IConsumer<string, string>? Consumer;
        private bool _initialized;
        private KafkaConfiguration? _configuration;
        private bool _disposed;

        public KafkaConsumer()
        {
        }

        public KafkaConsumer(KafkaConfiguration configuration)
        {
            Initialize(configuration);
        }

        public void Initialize(KafkaConfiguration configuration)
        {
            if (_initialized)
                throw new InvalidOperationException(KafkaConsumerIsAlreadyInitializedMessage);
            
            _configuration = configuration;
            
            ValidateConfiguration(_configuration);
            
            Consumer = BuildConsumer(configuration);
            
            _initialized = true;
        }
        
        public Task RunAsync(CancellationToken cancellationToken = default)
        {
            if (!_initialized)
                throw new InvalidOperationException(KafkaConsumerIsNotInitializedMessage);
            
            Task.Factory.StartNew(async () =>
                {
                    // KafkaConsumer is initialized and _configuration cannot be null
                    await StartConsumeLoop(_configuration!, cancellationToken);
                },
                cancellationToken, TaskCreationOptions.LongRunning, TaskScheduler.Default);

            return Task.CompletedTask;
        }

        private async Task StartConsumeLoop(KafkaConfiguration kafkaConfiguration, CancellationToken token)
        {
            // KafkaConsumer is initialized and Consumer cannot be null
            Consumer!.Subscribe(kafkaConfiguration.Topics);

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
                    await OnError(e, null);
                }
            }
            
            Consumer.Close();
            _disposed = true;
        }
        
        private void ValidateConfiguration(KafkaConfiguration? configuration)
        {
            if (configuration == null)
                throw new ArgumentNullException(nameof(KafkaConfiguration));

            var topics = configuration.Topics;
            
            if (topics == null)
                throw new ArgumentNullException(nameof(KafkaConfiguration.Topics));

            foreach (var topic in topics)
                if (topic == null)
                    throw new ArgumentNullException(nameof(topic));
        }

        private IConsumer<string, string> BuildConsumer(KafkaConfiguration configuration)
        {
            var consumerConfig = configuration as ConsumerConfig;

            return new ConsumerBuilder<string, string>(consumerConfig)
                .SetErrorHandler(configuration.ErrorHandler)
                .SetKeyDeserializer(configuration.KeyDeserializer)
                .SetValueDeserializer(configuration.ValueDeserializer)
                .SetLogHandler(configuration.LogHandler)
                .SetStatisticsHandler(configuration.StatisticsHandler)
                .SetOffsetsCommittedHandler(configuration.OffsetsCommittedHandler)
                .SetPartitionsAssignedHandler(configuration.PartitionsAssignedHandler)
                .SetPartitionsRevokedHandler(configuration.PartitionsRevokedHandler)
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