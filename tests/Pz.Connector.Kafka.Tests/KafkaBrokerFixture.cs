using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Testcontainers.Kafka;

namespace Pz.Connector.Kafka.Tests;

[CollectionDefinition("kafka")]
public sealed class KafkaCollection : ICollectionFixture<KafkaBrokerFixture>;

/// <summary>One KRaft broker per test run. Topic names are unique per call so facts never share
/// state. Helpers talk to the broker with Confluent's own clients, deliberately not through the
/// connector: a fact that used the code under test to seed and verify would prove nothing.</summary>
public sealed class KafkaBrokerFixture : IAsyncLifetime
{
    /// <summary>How many produce calls are in flight before the fixture waits on their delivery
    /// reports. Awaiting each record's report in turn would serialise a 150k-record seed into 150k
    /// broker round trips; the chunk keeps the queue well inside librdkafka's default 100k-message
    /// producer queue while still pipelining. Ordering within a partition is unaffected -- the
    /// idempotent producer preserves the enqueue order.</summary>
    private const int ProduceChunk = 5000;

    private KafkaContainer? _container;
    private IAdminClient? _admin;

    public string BootstrapServers { get; private set; } = "";

    public async Task InitializeAsync()
    {
        if (!DockerFacts.IsAvailable)
        {
            return;
        }

        // Built here rather than in a field initializer: Build() resolves and pings the docker
        // endpoint, so a constructor that built it would throw before the probe above could no-op --
        // and a collection fixture that throws is a failed fixture, not a skip. Testcontainers 4.15
        // retired the parameterless builder, so the image is a constructor argument;
        // apache/kafka-native is the KRaft image (no ZooKeeper, boots in a couple of seconds).
        // Auto-creation is off so a topic exists only because a fact created it: the unknown-topic
        // refusal is only a refusal if the broker does not quietly conjure the topic up first.
        _container = new KafkaBuilder("apache/kafka-native:3.9.1")
            .WithEnvironment("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false")
            .Build();
        await _container.StartAsync().ConfigureAwait(false);
        BootstrapServers = _container.GetBootstrapAddress();
        _admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = BootstrapServers }).Build();
    }

    public async Task DisposeAsync()
    {
        _admin?.Dispose();
        if (_container is not null)
        {
            await _container.DisposeAsync().ConfigureAwait(false);
        }
    }

    public Dictionary<string, object?> ConnectionConfig() => new() { ["bootstrap_servers"] = BootstrapServers };

    public async Task<string> CreateTopicAsync(int partitions, string? name = null)
    {
        name ??= "pz_" + Guid.NewGuid().ToString("N")[..12];
        await _admin!.CreateTopicsAsync([new TopicSpecification { Name = name, NumPartitions = partitions, ReplicationFactor = 1 }])
            .ConfigureAwait(false);
        await WaitForPartitionsAsync(name, partitions).ConfigureAwait(false);
        return name;
    }

    public async Task AddPartitionsAsync(string topic, int total)
    {
        await _admin!.CreatePartitionsAsync([new PartitionsSpecification { Topic = topic, IncreaseTo = total }]).ConfigureAwait(false);
        await WaitForPartitionsAsync(topic, total).ConfigureAwait(false);
    }

    /// <summary>Metadata propagation is asynchronous, so a produce issued the instant the admin call
    /// returns can be refused for a partition the cluster has not finished electing a leader for.
    /// Poll until every expected partition has one.</summary>
    private async Task WaitForPartitionsAsync(string topic, int partitions)
    {
        for (var attempt = 0; attempt < 100; attempt++)
        {
            var md = _admin!.GetMetadata(topic, TimeSpan.FromSeconds(5));
            if (md.Topics.Count == 1 && md.Topics[0].Error.Code == ErrorCode.NoError
                && md.Topics[0].Partitions.Count == partitions && md.Topics[0].Partitions.All(p => p.Leader >= 0))
            {
                return;
            }

            await Task.Delay(50).ConfigureAwait(false);
        }

        throw new InvalidOperationException($"topic {topic} did not report {partitions} ready partition(s)");
    }

    public Task DeleteTopicAsync(string topic) => _admin!.DeleteTopicsAsync([topic]);

    /// <summary>Raises the partition's low watermark to <paramref name="beforeOffset"/> the way
    /// retention does, on demand: every record below it is gone for good.</summary>
    public Task DeleteRecordsAsync(string topic, int partition, long beforeOffset) =>
        _admin!.DeleteRecordsAsync([new TopicPartitionOffset(topic, partition, new Offset(beforeOffset))]);

    public async Task ProduceAsync(string topic,
        IEnumerable<(int? Partition, byte[]? Key, byte[]? Value, IReadOnlyList<KeyValuePair<string, byte[]?>>? Headers)> records)
    {
        using var producer = new ProducerBuilder<byte[], byte[]>(new ProducerConfig
        {
            BootstrapServers = BootstrapServers, Acks = Acks.All, EnableIdempotence = true,
        }).Build();
        var inFlight = new List<Task<DeliveryResult<byte[], byte[]>>>(ProduceChunk);
        foreach (var (partition, key, value, headers) in records)
        {
            var message = new Message<byte[], byte[]> { Key = key!, Value = value! };
            if (headers is not null)
            {
                message.Headers = new Headers();
                foreach (var (name, bytes) in headers)
                {
                    message.Headers.Add(name, bytes!);
                }
            }

            inFlight.Add(partition is { } p
                ? producer.ProduceAsync(new TopicPartition(topic, p), message)
                : producer.ProduceAsync(topic, message));
            if (inFlight.Count == ProduceChunk)
            {
                await Task.WhenAll(inFlight).ConfigureAwait(false);
                inFlight.Clear();
            }
        }

        await Task.WhenAll(inFlight).ConfigureAwait(false);
        producer.Flush(TimeSpan.FromSeconds(30));
    }

    /// <summary>Simple text records round-robined by the broker (null key).</summary>
    public Task ProduceTextAsync(string topic, IEnumerable<string> values, int? partition = null) =>
        ProduceAsync(topic, values.Select(v => (partition, (byte[]?)null, (byte[]?)System.Text.Encoding.UTF8.GetBytes(v),
            (IReadOnlyList<KeyValuePair<string, byte[]?>>?)null)));

    /// <summary>Everything currently in the topic, from the beginning, all partitions, ordered by
    /// (partition, offset). Stops when every partition has reported EOF.</summary>
    public Task<List<ConsumeResult<byte[], byte[]>>> ConsumeAllAsync(string topic) => Task.Run(() =>
    {
        using var consumer = new ConsumerBuilder<byte[], byte[]>(new ConsumerConfig
        {
            BootstrapServers = BootstrapServers, GroupId = "fixture-" + Guid.NewGuid().ToString("N"),
            EnableAutoCommit = false, EnablePartitionEof = true, AutoOffsetReset = AutoOffsetReset.Earliest,
        }).Build();
        var partitions = _admin!.GetMetadata(topic, TimeSpan.FromSeconds(5)).Topics[0].Partitions
            .Select(p => new TopicPartitionOffset(topic, p.PartitionId, Offset.Beginning)).ToList();
        consumer.Assign(partitions);
        var pending = partitions.Count;
        var results = new List<ConsumeResult<byte[], byte[]>>();
        var deadline = DateTime.UtcNow.AddSeconds(60);
        while (pending > 0 && DateTime.UtcNow < deadline)
        {
            var r = consumer.Consume(TimeSpan.FromMilliseconds(200));
            if (r is null)
            {
                continue;
            }

            if (r.IsPartitionEOF)
            {
                pending--;
                continue;
            }

            results.Add(r);
        }

        consumer.Close();
        return results.OrderBy(r => r.Partition.Value).ThenBy(r => r.Offset.Value).ToList();
    });
}
