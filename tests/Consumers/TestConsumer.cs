using System;
using System.Threading.Tasks;
using Confluent.Kafka.Lib.Core.Consumers;

namespace Confluent.Kafka.Lib.Tests.Consumers
{
    class TestConsumer : KafkaConsumer<string, string>
    {
        protected override Task OnConsume(Message<string, string> message)
        {
            Console.WriteLine("Message Consumed.");
            
            return Task.CompletedTask;
        }

        protected override Task OnError(Exception exception, Message<string, string>? message)
        {
            Console.WriteLine("Error.");
            
            return Task.CompletedTask;
        }
    }
}