namespace Trendyol.Confluent.Kafka
{
    internal static class ErrorConstants
    {
        public const string KafkaConsumerIsAlreadyInitializedMessage =
            "KafkaConsumer is already initialized via its parameterized constructor.";

        public const string KafkaProducerIsAlreadyInitializedMessage =
            "KafkaProducer is already initialized via its parameterized constructor.";

        public const string KafkaConsumerIsNotInitializedMessage =
            "You have to initialize KafkaConsumer either by parameterized constructor or by calling Initialize method.";
        
        public const string KafkaProducerIsNotInitializedMessage =
            "You have to initialize KafkaProducer either by parameterized constructor or by calling Initialize method.";

        public const string ConfigurationBuilderCannotBeNullMessage = "Configuration builder cannot be null.";
    }
}