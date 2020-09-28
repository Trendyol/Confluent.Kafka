# Confluent.Kafka.Lib

This is a wrapper repository around Confluent .NET library to make clients use Kafka more conveniently.

Provides retry support.
Produces failed messages to error topic.
Can set commit period from `AddKafkaConsumer()` extension method.
Can set maximum retry count from `AddKafkaConsumer()` extension method.

## License
[MIT](https://choosealicense.com/licenses/mit/)
