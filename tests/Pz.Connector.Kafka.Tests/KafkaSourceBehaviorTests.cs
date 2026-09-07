using Apache.Arrow;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Kafka.Tests;

[Collection("kafka")]
[Trait("Category", "Docker")]
public sealed class KafkaSourceBehaviorTests
{
    private readonly KafkaBrokerFixture _broker;

    public KafkaSourceBehaviorTests(KafkaBrokerFixture broker)
    {
        _broker = broker;
        DockerFacts.SkipUnlessDocker();
    }

    private ConnectorConfig Config => new(_broker.ConnectionConfig());

    [SkippableFact]
    public async Task First_run_from_earliest_lands_everything_and_emits_the_bound_as_token()
    {
        var topic = await _broker.CreateTopicAsync(partitions: 3);
        await _broker.ProduceTextAsync(topic, ["a", "b"], partition: 0);
        await _broker.ProduceTextAsync(topic, ["c"], partition: 2);

        var (rows, token) = await ReadAsync(new KafkaConnector(), Config, Spec(topic));

        Assert.Equal(3, rows.Count);
        Assert.Equal(["a", "b", "c"], rows.OrderBy(r => r.Partition).ThenBy(r => r.Offset).Select(r => r.Value));
        Assert.Equal($$$"""{"v":1,"topic":"{{{topic}}}","partitions":{"0":2,"1":0,"2":1}}""", token);
    }

    /// <summary>Drains the single partition: (rows as (partition, offset, value), token candidate).</summary>
    internal static async Task<(List<(int Partition, long Offset, string? Value)> Rows, string? Token)> ReadAsync(
        ISourceConnector connector, ConnectorConfig config, DatasetSpec spec, CancellationToken ct = default)
    {
        await using var source = await connector.OpenAsync(config, ct);
        var partitions = await source.PlanReadAsync(spec, ReadHints.None, ct);
        var partition = Assert.Single(partitions);
        var rows = new List<(int, long, string?)>();
        await foreach (var batch in partition.ReadAsync(BatchOptions.Default, ct))
        {
            using (batch)
            {
                var p = (Int32Array)batch.Column(1);
                var o = (Int64Array)batch.Column(2);
                var v = (StringArray)batch.Column(5);
                for (var i = 0; i < batch.Length; i++)
                {
                    rows.Add((p.GetValue(i)!.Value, o.GetValue(i)!.Value, v.IsNull(i) ? null : v.GetString(i)));
                }
            }
        }

        var sync = Assert.IsAssignableFrom<ISyncStatePartition>(partition);
        return (rows, sync.TryGetSyncStateCandidate(out var token) ? token : null);
    }

    private static DatasetSpec Spec(string topic, string? prior = null, Dictionary<string, object?>? options = null) =>
        new("kafka", topic, options ?? []) { PriorSyncState = prior };
}
