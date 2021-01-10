# Confluent.Kafka.Lib

A wrapper consumer around Confluent .NET `IConsumer<string, string>` to make easier use of Kafka consumers.

# Usage

Implement a consumer class deriving from `KafkaConsumer`:
``` cs
class EventConsumer : KafkaConsumer
{
    protected override async Task OnConsume(ConsumeResult<string, string> result)
    {
        await DoWork(result);
    }

    protected override async Task OnError(Exception exception, ConsumeResult<string, string>? result)
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
``` cs
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
``` cs
await consumer.RunAsync();
```
or
``` cs
var cts = new CancellationTokenSource();
await consumer.RunAsync(cts.Token);
```

# Usage via dependency injection
Register your `KafkaConsumer` using `AddKafkaConsumer` extension method:
``` cs
services.AddKafkaConsumer<MyConsumer>(configuration =>
            {
                configuration.Topics = new[] {"MyTopic"};
                configuration.GroupId = "MyGroup";
                configuration.BootstrapServers = "BOOTSTRAP_SERVERS";
            });
```
And use all your registered services in your derived consumer:
``` cs
using System;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace TestApplication
{
    public class MyConsumer : KafkaConsumer
    {
        private readonly IService _service;

        public MyConsumer(IService service)
        {
            _service = service;
        }

        protected override async Task OnConsume(ConsumeResult<string, string> result)
        {
            await _service.DoWorkAsync(result);
        }

        protected override async Task OnError(Exception exception, ConsumeResult<string, string>? result)
        {
            await _service.DoWorkForExceptionAsync(exception, result);
        }
    }
}
```

# Installation
Installing via NuGet will soon be available.
For the time being, you can download the source here and use it in your project.

## License
[MIT](https://choosealicense.com/licenses/mit/)
