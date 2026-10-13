using Confluent.Kafka;

namespace Pz.Connector.Kafka;

/// <summary>The one place Confluent client builders are called, so tests can substitute clients
/// and so every client carries the same base property map.</summary>
internal interface IKafkaClientFactory
{
    IConsumer<byte[], byte[]> CreateConsumer(IReadOnlyDictionary<string, string> properties, string groupId, KafkaStatsBridge? stats = null);

    IProducer<byte[], byte[]> CreateProducer(IReadOnlyDictionary<string, string> properties, string compression, KafkaStatsBridge? stats = null);

    IAdminClient CreateAdmin(IReadOnlyDictionary<string, string> properties);
}

internal sealed class KafkaClientFactory : IKafkaClientFactory
{
    public static readonly KafkaClientFactory Instance = new();

    public IConsumer<byte[], byte[]> CreateConsumer(IReadOnlyDictionary<string, string> properties, string groupId, KafkaStatsBridge? stats = null)
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
        WithStatistics(config, stats);
        var builder = new ConsumerBuilder<byte[], byte[]>(config);
        if (stats is not null)
        {
            builder.SetStatisticsHandler((_, json) => stats.OnStatistics(json));
        }

        return builder.Build();
    }

    public IProducer<byte[], byte[]> CreateProducer(IReadOnlyDictionary<string, string> properties, string compression, KafkaStatsBridge? stats = null)
    {
        var config = new ProducerConfig(Copy(properties))
        {
            EnableIdempotence = true,
            Acks = Acks.All,
            LingerMs = 5,
        };
        config.Set("compression.type", compression);
        WithStatistics(config, stats);
        var builder = new ProducerBuilder<byte[], byte[]>(config);
        if (stats is not null)
        {
            builder.SetStatisticsHandler((_, json) => stats.OnStatistics(json));
        }

        return builder.Build();
    }

    public IAdminClient CreateAdmin(IReadOnlyDictionary<string, string> properties) =>
        new AdminClientBuilder(new AdminClientConfig(Copy(properties))).Build();

    /// <summary>Statistics are off in librdkafka until an interval is set. A user's own
    /// `statistics.interval.ms` (including 0, which turns them off) is left as written.</summary>
    private static void WithStatistics(ClientConfig config, KafkaStatsBridge? stats)
    {
        if (stats is not null && config.Get("statistics.interval.ms") is null)
        {
            config.StatisticsIntervalMs = 1000;
        }
    }

    private static Dictionary<string, string> Copy(IReadOnlyDictionary<string, string> properties) =>
        properties.ToDictionary(kv => kv.Key, kv => kv.Value, StringComparer.Ordinal);
}
