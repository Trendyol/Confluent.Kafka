using System;
using System.Threading;
using System.Threading.Tasks;

namespace Confluent.Kafka.Utility
{
    public abstract class KafkaConsumer : IKafkaConsumer
    {
        protected IConsumer<string, string> Consumer;
        
        public Task RunAsync(KafkaConfiguration configuration, 
            CancellationToken cancellationToken = default)
        {
            Consumer = BuildConsumer(configuration);
            
            Task.Factory.StartNew(async () =>
                {
                    await StartConsumeLoop(configuration, cancellationToken);
                },
                cancellationToken, TaskCreationOptions.LongRunning, TaskScheduler.Default);

            return Task.CompletedTask;
        }

        private async Task StartConsumeLoop(KafkaConfiguration kafkaConfiguration, CancellationToken token)
        {
            Consumer.Subscribe(kafkaConfiguration.Topics);

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
                    {
                        continue;
                    }

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
    }
}