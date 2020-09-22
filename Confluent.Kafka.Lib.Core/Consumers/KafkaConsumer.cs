using System;

namespace Confluent.Kafka.Lib.Core
{
    public class KafkaConsumer : IKafkaConsumer, IDisposable
    {
        private bool _disposed;
        private readonly IConsumer<string, string> _consumer;
        
        public KafkaConsumer(ConsumerConfig config)
        {
            var builder = new ConsumerBuilder<string, string>(config);

            _consumer = builder.Build();
        }

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