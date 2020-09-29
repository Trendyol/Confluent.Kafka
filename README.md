# Confluent.Kafka.Lib

This is a wrapper repository around Confluent .NET library to make clients use Kafka more conveniently.

* Provides retry support.
* Produces failed messages to error topic.
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
                "test-bootstrap-servers",
                "test-topic",
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

        protected override Task OnConsume(ConsumeResult<string, string> consumeResult)
        {
            var key = consumeResult.Message.Key;
            var value = consumeResult.Message.Value;
            _logger.LogInformation($"Key : {key}, Value : {value}");
            
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
