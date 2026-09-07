using Confluent.Kafka;

namespace Pz.Connector.Kafka;

/// <summary>The one place Confluent client builders are called, so tests can substitute clients
/// and so every client carries the same base property map.</summary>
internal interface IKafkaClientFactory
{
    IConsumer<byte[], byte[]> CreateConsumer(IReadOnlyDictionary<string, string> properties, string groupId);

    IProducer<byte[], byte[]> CreateProducer(IReadOnlyDictionary<string, string> properties, string compression);

    IAdminClient CreateAdmin(IReadOnlyDictionary<string, string> properties);
}

internal sealed class KafkaClientFactory : IKafkaClientFactory
{
    public static readonly KafkaClientFactory Instance = new();

    public IConsumer<byte[], byte[]> CreateConsumer(IReadOnlyDictionary<string, string> properties, string groupId)
    {
        var config = new ConsumerConfig(Copy(properties))
        {
            GroupId = groupId,
            EnableAutoCommit = false,
            EnablePartitionEof = true,
            // Assign-only consumption: the stored token decides where to start, never the group's
            // committed position, so a missing offset is an error, not a silent reset.
            AutoOffsetReset = AutoOffsetReset.Error,
        };
        return new ConsumerBuilder<byte[], byte[]>(config).Build();
    }

    public IProducer<byte[], byte[]> CreateProducer(IReadOnlyDictionary<string, string> properties, string compression)
    {
        var config = new ProducerConfig(Copy(properties))
        {
            EnableIdempotence = true,
            Acks = Acks.All,
            LingerMs = 5,
        };
        config.Set("compression.type", compression);
        return new ProducerBuilder<byte[], byte[]>(config).Build();
    }

    public IAdminClient CreateAdmin(IReadOnlyDictionary<string, string> properties) =>
        new AdminClientBuilder(new AdminClientConfig(Copy(properties))).Build();

    private static Dictionary<string, string> Copy(IReadOnlyDictionary<string, string> properties) =>
        properties.ToDictionary(kv => kv.Key, kv => kv.Value, StringComparer.Ordinal);
}
