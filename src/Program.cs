using Confluent.Kafka;

class Program
{
    private static readonly string BootstrapServers = "127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092";
    private static readonly string Topic = "my_topic";

    static async Task Main(string[] args)
    {
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = BootstrapServers,
            Acks = Acks.All, // at least once
            ClientId = "KafkaExampleProducer"
        };

        using (var producer = new ProducerBuilder<string, string>(producerConfig)
                   .SetKeySerializer(Serializers.Utf8)
                   .SetValueSerializer(Serializers.Utf8)
                   .Build())
        {
            Console.Write("Enter a key: ");
            var key = Console.ReadLine();

            Console.Write("Enter a value: ");
            var value = Console.ReadLine();

            var message = new Message<string, string> { Key = key, Value = value };
            var deliveryReport = await producer.ProduceAsync(Topic, message);
            Console.WriteLine($"Produced message to {deliveryReport.Topic} partition {deliveryReport.Partition} @ offset {deliveryReport.Offset}");
        }

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = BootstrapServers,
            GroupId = "my_consumer_group",
            EnableAutoCommit = false // auto-commit is disabled for at most once
        };

        using (var consumer = new ConsumerBuilder<string, string>(consumerConfig)
                   .SetKeyDeserializer(Deserializers.Utf8)
                   .SetValueDeserializer(Deserializers.Utf8)
                   .Build())
        {
            consumer.Subscribe(Topic);
            while (true)
            {
                try
                {
                    var consumeResult = consumer.Consume();
                    Console.WriteLine($"Received message: {consumeResult.Message.Value}");

                    consumer.Commit(consumeResult);
                }
                catch (ConsumeException e)
                {
                    Console.WriteLine($"Error consuming message: {e.Error.Reason}");
                }
            }
        }
    }
}
