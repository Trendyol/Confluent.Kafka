using System;
using Confluent.Kafka;

namespace Trendyol.Confluent.Kafka
{
    public class KafkaProducerConfig : ProducerConfig
    {
        protected internal Action<IProducer<string, string>, Error> ErrorHandler { get; set; }
        protected internal Action<IProducer<string, string>, LogMessage> LogHandler { get; set; }
        protected internal Action<IProducer<string, string>, string> StatisticsHandler { get; set; }
        protected internal Action<IProducer<string, string>, string> OAuthBearerTokenRefreshHandler { get; set; }
        protected internal ISerializer<string> KeySerializer { get; set; }
        protected internal ISerializer<string> ValueSerializer { get; set; }
        protected internal IAsyncSerializer<string> AsyncKeySerializer { get; set; }
        protected internal IAsyncSerializer<string> AsyncValueSerializer { get; set; }
    }
}