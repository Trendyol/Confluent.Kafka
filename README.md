# Confluent.Kafka.Lib

A wrapper consumer around Confluent .NET `IConsumer<string, string>` to make easier use of Kafka consumers.

# Usage

Implement a consumer class deriving from `KafkaConsumer`:
``` cs
class EventConsumer : KafkaConsumer
{
    protected override Task OnConsume(ConsumeResult<string, string> result)
    {
        await DoWork(result);
    }

    protected override Task OnError(Exception exception, ConsumeResult<string, string>? result)
    {
        await DoWorkForException(exception, result);
    }
}
```

You can create an instance of your `EventConsumer` via parameterized constructor:
``` cs
var config = new KafkaConfiguration()
{
    Topics = new []{ "MyEvent" },
    BootstrapServers = "BOOTSTRAP_SERVERS",
    GroupId = "myEventGroup",
};
var consumer = new EventConsumer(config);
```
or via using default constructor and `Initialize(config)` method:
```
var config = new KafkaConfiguration()
{
    Topics = new []{ "MyEvent" },
    BootstrapServers = "BOOTSTRAP_SERVERS",
    GroupId = "myEventGroup",
};
var consumer = new EventConsumer();
consumer.Initialize(config);
```

And then start your consumer via `RunAsync` method, you can either give a `CancellationToken` or use the default token:
```
await consumer.RunAsync();
```
or
```
var cts = new CancellationTokenSource();
await consumer.RunAsync(cts.Token);
```

## License
[MIT](https://choosealicense.com/licenses/mit/)
