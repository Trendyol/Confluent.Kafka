namespace Confluent.Kafka.Lib.Example
{
    public class Event
    {
        public int Id { get; set; }
        public string? Name { get; set; }

        public override string ToString()
        {
            return $"Id : { Id }, Name : { Name }";
        }
    }

    public class Event2
    {
        public int Id { get; set; }
    }
}