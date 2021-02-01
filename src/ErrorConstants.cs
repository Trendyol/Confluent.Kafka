namespace Trendyol.Confluent.Kafka
{
    internal static class ErrorConstants
    {
        public const string KafkaConsumerIsAlreadyInitializedMessage =
            "KafkaConsumer is already initialized.";
        public const string KafkaConsumerIsNotInitializedMessage =
            "You have to initialize KafkaConsumer either by parameterized constructor or by Initialize method.";
        public const string ConfigurationBuilderCannotBeNullMessage = "Configuration builder cannot be null.";
    }
}