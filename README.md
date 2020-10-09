# Confluent.Kafka.Lib

This is a wrapper repository around Confluent .NET library to make clients use Kafka more conveniently.

* Provides retries with *at least once semantics*, your services should be idempotent.
* Produces messages that could not be processed to your *RetryTopic* parameter.
* Uses different consumers for main consumer and retry consumer.
* Provides periodic runs every *RetryPeriod* timespan through records in retry topic and reprocessing them.
* Produces failed messages (exceeded *MaxRetryCount* parameter) to your *FailedTopic* parameter.
* If using manual commit, you can set commit period in your *KafkaConfig*.
* You can set *MaxRetryCount* for your message in your *KafkaConfig*.

You can open issues and all PRs are welcome.

# Usage

You can register your KafkaConsumer from `ConfigureServices` method in your `Startup.cs`:

``` cs
public void ConfigureServices(IServiceCollection services)
{
    services.AddKafkaConsumer<TestConsumer>(new KafkaConfig
    {
        Topic = "MY-TOPIC-MAIN",                  // Main consumer subscribes to this topic
        RetryTopic = "MY-TOPIC-RETRY",            // Retry consumer subscribes to this topic
        FailedTopic = "MY-TOPIC-FAILED",          // Retry producer produces to this topic if message processing is eventually failed
        RetryPeriod = TimeSpan.FromSeconds(10),   // Run retry every 10 seconds
        MainConsumerConfig = new ConsumerConfig   // Provides configuration for your main consumer
        {
            GroupId = "dogac-consumer-group",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            BootstrapServers = "localhost:9092"
        },
        RetryConsumerConfig = new ConsumerConfig  // Provides configuration for your retry consumer
        {
            GroupId = "dogac-consumer-group",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            BootstrapServers = "localhost:9092",
        },
        MaxRetryCount = 3,                        // Maximum retry count for re-processing records.
        RetryProducerConfig = new ProducerConfig  // Provides configuration for your producer that produces to main and retry topics
        {
            BootstrapServers = "localhost:9092"
        }                                         // Note that we didn't set commit period because by default consumer config enables auto-commit
    });
}
```

Creating a new `KafkaConsumer`:

``` cs
public class TestConsumer : KafkaConsumer
{
    protected override Task OnConsume(Message<string, string> message)
    {
        Console.WriteLine($"Key : {message.Key}, Value : {message.Value}");
        
        return Task.CompletedTask;
    }

    protected override Task OnError(Exception exception)
    {
        // Handle your errors which occurs in your OnConsume method
        // No retry semantics for consumers is needed because it is handled in base KafkaConsumer
        
        return Task.CompletedTask;
    }
}
```

## License
[MIT](https://choosealicense.com/licenses/mit/)
