using System;
using System.Threading.Tasks;
using Confluent.Kafka.Lib.Core.Consumers;
using Microsoft.Extensions.Logging;

namespace Confluent.Kafka.Lib.Example
{
    public class TestConsumer : KafkaConsumer
    {
        protected override Task OnConsume(Message<string, string> message)
        {
            Console.WriteLine($"Key : {message.Key}, Value : {message.Value}");
            
            return Task.CompletedTask;
        }

        protected override Task OnError(Exception exception)
        {
            Console.WriteLine(exception.ToString());
            
            return Task.CompletedTask;
        }
    }
}