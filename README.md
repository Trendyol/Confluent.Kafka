# Confluent.Kafka.Lib

This is a wrapper repository around Confluent .NET library to make clients use Kafka more conveniently.

* Provides retries with *at least once semantics*, your services should be idempotent.
* Produces messages that could not be processed to "your-topic.retry" topic.
* Uses different consumers in a single consumer group for main consumer and retry consumers.
* Provides periodic runs every 15 minutes through retry topic and reprocessing them.
* Produces failed messages (exceeded max retry count) to "your-topic.failed" topic.
* Can set commit period from `AddKafkaConsumer()` extension method.
* Can set maximum retry count from `AddKafkaConsumer()` extension method.

You can open issues and all PRs are welcome.

# Usage

You can register your KafkaConsumer from `ConfigureServices` method in your `Startup`:

``` cs
public void ConfigureServices(IServiceCollection services)
        {
            services.AddHealthChecks();
            
            services.AddKafkaConsumer<TestConsumer>(
                "test-topic",
                "test-bootstrap-servers",
                "test-group-id"); // Set other options default
        }
```

Creating a new `KafkaConsumer`:

``` cs
public class TestConsumer : KafkaConsumer
    {
        private readonly ILogger<TestConsumer> _logger;

        public TestConsumer(ILogger<TestConsumer> logger)
        {
            _logger = logger;
        }

        protected override Task OnConsume(Message<string, string> message)
        {
            _logger.LogInformation($"Key : {message.Key}, Value : {message.Value}");
            
            return Task.CompletedTask;
        }

        protected override Task OnError(Exception exception)
        {
            // Logging will be enough
            // Fault tolerance in messages is handled in base consumer
            _logger.LogError(exception.ToString()); 
            
            return Task.CompletedTask;
        }
    }
```

## License
[MIT](https://choosealicense.com/licenses/mit/)
