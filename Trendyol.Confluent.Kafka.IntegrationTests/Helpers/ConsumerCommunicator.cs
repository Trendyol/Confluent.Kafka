using System;
using System.Threading;

namespace Trendyol.Confluent.Kafka.Tests.Helpers
{
    public static class ConsumerCommunicator
    {
        public static readonly ManualResetEvent ManualResetEvent 
            = new ManualResetEvent(false);

        public static void WaitForMessageToBeConsumed(TimeSpan timeout)
        {
            ManualResetEvent.WaitOne(timeout);
        }
        
        public static void InformMessageConsumed()
        {
            ManualResetEvent.Set();
        }
    }
}