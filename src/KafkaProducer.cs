using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace Trendyol.Confluent.Kafka
{
    public class KafkaProducer : IKafkaProducer
    {
        protected IProducer<string, string> Producer;
        private bool _initialized;

        public KafkaProducer()
        {
        }

        public KafkaProducer(KafkaProducerConfig config)
        {
            BuildKafkaProducer(config);
        }

        public void Initialize(KafkaProducerConfig config)
        {
            if (_initialized)
                throw new InvalidOperationException(ErrorConstants.KafkaProducerIsAlreadyInitializedMessage);

            BuildKafkaProducer(config);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void CheckIfNotInitialized()
        {
            if (!_initialized)
            {
                throw new InvalidOperationException(ErrorConstants.KafkaProducerIsNotInitializedMessage);
            }
        }

        private void BuildKafkaProducer(KafkaProducerConfig producerConfig)
        {
            var kafkaConfig = producerConfig as ProducerConfig;
            var producer = new ProducerBuilder<string, string>(kafkaConfig)
                .SetErrorHandler(producerConfig.ErrorHandler)
                .SetKeySerializer(producerConfig.KeySerializer)
                .SetLogHandler(producerConfig.LogHandler)
                .SetStatisticsHandler(producerConfig.StatisticsHandler)
                .SetSerializers(producerConfig)
                .SetOAuthBearerTokenRefreshHandler(producerConfig.OAuthBearerTokenRefreshHandler)
                .Build();
            Producer = producer;
            _initialized = true;
        }

        public void Dispose()
        {
            CheckIfNotInitialized();
            
            Producer.Dispose();
        }

        public int AddBrokers(string brokers)
        {
            CheckIfNotInitialized();
            
            return Producer.AddBrokers(brokers);
        }

        public Handle Handle
        {
            get
            {
                CheckIfNotInitialized();
                
                return Producer.Handle;
            }
        }

        public string Name
        {
            get
            {
                CheckIfNotInitialized();
                
                return Producer.Name;
            }
        }

        public Task<DeliveryResult<string, string>> ProduceAsync(string topic, 
            Message<string, string> message, 
            CancellationToken cancellationToken = new CancellationToken())
        {
            CheckIfNotInitialized();
            
            return Producer.ProduceAsync(topic, message, cancellationToken);
        }

        public Task<DeliveryResult<string, string>> ProduceAsync(TopicPartition topicPartition, 
            Message<string, string> message,
            CancellationToken cancellationToken = new CancellationToken())
        {
            CheckIfNotInitialized();
            
            return Producer.ProduceAsync(topicPartition, message, cancellationToken);
        }

        public void Produce(string topic, 
            Message<string, string> message, 
            Action<DeliveryReport<string, string>> deliveryHandler = null)
        {
            CheckIfNotInitialized();
            
            Producer.Produce(topic, message, deliveryHandler);
        }

        public void Produce(TopicPartition topicPartition, 
            Message<string, string> message, 
            Action<DeliveryReport<string, string>> deliveryHandler = null)
        {
            CheckIfNotInitialized();
            
            Producer.Produce(topicPartition, message, deliveryHandler);
        }

        public int Poll(TimeSpan timeout)
        {
            CheckIfNotInitialized();
            
            return Producer.Poll(timeout);
        }

        public int Flush(TimeSpan timeout)
        {
            CheckIfNotInitialized();
            
            return Producer.Flush(timeout);
        }

        public void Flush(CancellationToken cancellationToken = new CancellationToken())
        {
            CheckIfNotInitialized();
            
            Producer.Flush(cancellationToken);
        }

        public void InitTransactions(TimeSpan timeout)
        {
            CheckIfNotInitialized();
            
            Producer.InitTransactions(timeout);
        }

        public void BeginTransaction()
        {
            CheckIfNotInitialized();
            
            Producer.BeginTransaction();
        }

        public void CommitTransaction(TimeSpan timeout)
        {
            CheckIfNotInitialized();
            
            Producer.CommitTransaction(timeout);
        }

        public void CommitTransaction()
        {
            CheckIfNotInitialized();

            Producer.CommitTransaction();
        }

        public void AbortTransaction(TimeSpan timeout)
        {
            CheckIfNotInitialized();
            
            Producer.AbortTransaction(timeout);
        }

        public void AbortTransaction()
        {
            CheckIfNotInitialized();
            
            Producer.AbortTransaction();
        }

        public void SendOffsetsToTransaction(IEnumerable<TopicPartitionOffset> offsets, 
            IConsumerGroupMetadata groupMetadata, 
            TimeSpan timeout)
        {
            CheckIfNotInitialized();
            
            Producer.SendOffsetsToTransaction(offsets, groupMetadata, timeout);
        }
    }
}