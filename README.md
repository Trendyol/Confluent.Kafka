# Confluent.Kafka.Lib

A wrapper consumer around Confluent .NET `IConsumer<,>` to make easier use of Kafka.

# Usage

Implement a consumer class deriving from `KafkaConsumer<TKey, TValue>`:
``` cs
public class KafkaConsumerImpl : KafkaConsumer<string, string>
{
    public KafkaConsumerImpl(string topic, IConsumer<string, string> consumer) : base(topic, consumer)
    {
    }

    protected override Task ProcessRecord(ConsumeResult<string, string> result)
    {
        throw new NotImplementedException();
    }

    protected override Task OnProcessError(Exception exception, ConsumeResult<string, string> result)
    {
        throw new NotImplementedException();
    }

    protected override Task OnConsumeError(ConsumeException exception)
    {
        throw new NotImplementedException();
    }

    protected override Task OnCommitError(KafkaException exception, ConsumeResult<string, string> result)
    {
        throw new NotImplementedException();
    }
}
```

Run consumer like this:
``` cs
var consumer = new KafkaConsumerImpl(
                "TOPIC",
                new ConsumerBuilder<string, string>(
                    new ConsumerConfig
                {
                    BootstrapServers = "BOOTSTRAP_SERVERS",
                    GroupId = "GROUP_ID"
                }).Build());

var cts = new CancellationTokenSource();

await consumer.RunAsync(cts.Token);
```

## License
[MIT](https://choosealicense.com/licenses/mit/)
