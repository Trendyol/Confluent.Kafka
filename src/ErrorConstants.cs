namespace Confluent.Kafka.Utility
{
    public class ErrorConstants
    {
        public const string KafkaConsumerIsAlreadyInitializedMessage =
            "KafkaConsumer is already initialized.";
        public const string KafkaConsumerIsNotInitializedMessage =
            "You have to initialize KafkaConsumer either by parameterized constructor or by Initialize method.";
    }
}