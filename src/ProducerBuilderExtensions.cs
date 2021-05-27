using Confluent.Kafka;

namespace Trendyol.Confluent.Kafka
{
    internal static class ProducerBuilderExtensions
    {
        public static ProducerBuilder<string, string> SetSerializers(this ProducerBuilder<string, string> builder,
            KafkaProducerConfig producerConfig)
        {
            if (producerConfig.KeySerializer != null)
                builder.SetKeySerializer(producerConfig.KeySerializer);
            if (producerConfig.ValueSerializer != null)
                builder.SetValueSerializer(producerConfig.ValueSerializer);
            if (producerConfig.AsyncKeySerializer != null)
                builder.SetKeySerializer(producerConfig.AsyncKeySerializer);
            if (producerConfig.AsyncValueSerializer != null)
                builder.SetValueSerializer(producerConfig.AsyncValueSerializer);

            return builder;
        }
    }
}