using System;
using System.Threading.Tasks;
using Confluent.Kafka.Lib.Core.Consumers;

namespace Confluent.Kafka.Lib.Example
{
    public class TestConsumer : KafkaConsumer<Ignore, Event>
    {
        protected override Task OnConsume(Message<Ignore, Event> message)
        {
            Console.WriteLine($"Key: {message.Key}, Value : {message.Value}");
            
            return Task.CompletedTask;
        }

        protected override Task OnError(Exception exception)
        {
            Console.WriteLine(exception);
            
            return Task.CompletedTask;
        }
    }
}