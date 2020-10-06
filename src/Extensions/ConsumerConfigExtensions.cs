namespace Confluent.Kafka.Lib.Core.Extensions
{
    public static class ConsumerConfigExtensions
    {
        public static bool IsAutoCommitEnabled(this ConsumerConfig config)
        {
            return config.EnableAutoCommit == null ||
                   config.EnableAutoCommit == true;
        }
    }
}