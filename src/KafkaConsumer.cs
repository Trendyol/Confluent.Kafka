using System;
using System.Threading;
using System.Threading.Tasks;

namespace Confluent.Kafka.Lib.Core
{
    public abstract class KafkaConsumer<TKey, TValue> : IKafkaConsumer
    {
        private bool _disposed;
        private readonly string _topic;
        private readonly IConsumer<TKey, TValue> _consumer;

        protected KafkaConsumer(string topic,
            IConsumer<TKey, TValue> consumer)
        {
            _topic = topic ?? throw new ArgumentNullException(nameof(topic));
            _consumer = consumer;
        }

        public Task RunAsync(CancellationToken cancellationToken = default)
        {
            Task.Factory.StartNew(async () => await StartConsumerLoop(cancellationToken),
                cancellationToken, TaskCreationOptions.LongRunning, TaskScheduler.Default);

            return Task.CompletedTask;
        }

        private async Task StartConsumerLoop(CancellationToken token)
        {
            _consumer.Subscribe(_topic);

            while (!token.IsCancellationRequested)
            {
                try
                {
                    var result = null as ConsumeResult<TKey, TValue>;

                    try
                    {
                        result = _consumer.Consume(token);
                    }
                    catch (ConsumeException e)
                    {
                        await OnConsumeError(e);
                        continue;
                    }

                    if (result.Message == null)
                    {
                        continue;
                    }

                    try
                    {
                        await ProcessRecord(result);
                    }
                    catch (Exception e)
                    {
                        await OnProcessError(e, result);
                    }

                    try
                    {
                        _consumer.Commit(result);
                    }
                    catch (KafkaException e)
                    {
                        await OnCommitError(e, result);
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
            }
        }
        
        protected abstract Task ProcessRecord(ConsumeResult<TKey, TValue> result);
        protected abstract Task OnProcessError(Exception exception, ConsumeResult<TKey, TValue> result);
        protected abstract Task OnConsumeError(ConsumeException exception);
        protected abstract Task OnCommitError(KafkaException exception, ConsumeResult<TKey, TValue> result);

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;

            if (disposing)
            {
                _consumer.Close();
            }
        }
    }
}