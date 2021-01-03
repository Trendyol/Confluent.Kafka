# Confluent.Kafka.Lib

A wrapper consumer around Confluent .NET `IConsumer<,>` to make easier use of Kafka.

# Usage

Implement a consumer class deriving from `KafkaConsumer`:
``` cs
class OrderCreatedEventConsumer : KafkaConsumer
{
    protected override Task OnConsume(ConsumeResult<string, string> result)
    {
        throw new NotImplementedException();
    }

    protected override Task OnError(Exception exception, ConsumeResult<string, string>? result)
    {
        throw new NotImplementedException();
    }
}
```

Run your consumer like this:
``` cs
var consumer = new OrderCreatedEventConsumer();
var config = new KafkaConfiguration()
{
    Topics = new []{ "OrderCreatedEvent" },
    BootstrapServers = "BOOTSTRAP_SERVERS",
    GroupId = "orderCreatedEventListener",
};

await consumer.RunAsync(config);
```

## License
[MIT](https://choosealicense.com/licenses/mit/)
