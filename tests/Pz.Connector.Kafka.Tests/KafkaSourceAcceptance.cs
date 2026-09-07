using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit;

namespace Pz.Connector.Kafka.Tests;

/// <summary>TestKit source contract against the Testcontainers broker. SmallDataset is a 2-partition
/// topic with 120 ~100-byte records (>= 100 rows, >= 2 batches under the suite's 4KB target).
/// LargeDataset is a 1-partition topic with 150k records so mid-read cancellation is observable.
/// Both topics are created once per fixture on first use. No BoundedWindow/Checkpoint/ChangeCapture
/// fixtures: the connector declares none of those capabilities.</summary>
[Collection("kafka")]
[Trait("Category", "Docker")]
public sealed class KafkaSourceAcceptance : SourceConnectorAcceptanceTests
{
    private static readonly SemaphoreSlim Seed = new(1, 1);
    private static string? _small;
    private static string? _large;
    private readonly KafkaBrokerFixture _broker;

    public KafkaSourceAcceptance(KafkaBrokerFixture broker)
    {
        _broker = broker;
        DockerFacts.SkipUnlessDocker();
        SeedAsync().GetAwaiter().GetResult();
    }

    protected override ConnectorConfig ValidConfig => new(_broker.ConnectionConfig());

    protected override DatasetSpec SmallDataset => new("kafka", _small!, new Dictionary<string, object?>());

    protected override DatasetSpec? LargeDataset => new("kafka", _large!, new Dictionary<string, object?>());

    protected override void GateFact() => DockerFacts.SkipUnlessDocker();

    protected override ISourceConnector CreateSource() => new KafkaConnector();

    private async Task SeedAsync()
    {
        await Seed.WaitAsync();
        try
        {
            _small ??= await SeedTopicAsync(partitions: 2, rows: 120);
            _large ??= await SeedTopicAsync(partitions: 1, rows: 150_000);
        }
        finally
        {
            Seed.Release();
        }
    }

    private async Task<string> SeedTopicAsync(int partitions, int rows)
    {
        var topic = await _broker.CreateTopicAsync(partitions);
        await _broker.ProduceTextAsync(topic, Enumerable.Range(0, rows).Select(i => $"{{\"id\":{i},\"pad\":\"{new string('x', 80)}\"}}"));
        return topic;
    }
}
