using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Kafka.Tests;

[Collection("kafka")]
[Trait("Category", "Docker")]
public sealed class KafkaStatsIntegrationTests
{
    private readonly KafkaBrokerFixture _broker;

    public KafkaStatsIntegrationTests(KafkaBrokerFixture broker)
    {
        _broker = broker;
        DockerFacts.SkipUnlessDocker();
    }

    [SkippableFact]
    public async Task A_read_that_closes_before_the_first_statistics_tick_still_reports()
    {
        var topic = await _broker.CreateTopicAsync(partitions: 1);
        await _broker.ProduceTextAsync(topic, ["only"]);
        ISourceConnector connector = new KafkaConnector();
        await using var source = await connector.OpenAsync(
            new ConnectorConfig(_broker.ConnectionConfig()), CancellationToken.None);

        var acc = new ConnectorStatsAccumulator();
        using (ConnectorStats.Use(acc))
        {
            var partitions = await source.PlanReadAsync(
                new DatasetSpec("kafka", topic, new Dictionary<string, object?>()), ReadHints.None, CancellationToken.None);
            await foreach (var batch in partitions[0].ReadAsync(new BatchOptions(), CancellationToken.None))
            {
                batch.Dispose();
            }
        }

        var s = acc.Snapshot();
        Assert.True(s.Requests > 0);
        Assert.Contains(new KeyValuePair<string, double>("kafka.messages", 1), s.Extras);
    }

    [SkippableFact]
    public async Task A_write_reports_by_the_time_commit_returns()
    {
        var topic = await _broker.CreateTopicAsync(partitions: 1);
        var schema = new Schema([new Field("id", Int64Type.Default, true)], null);
        ISinkConnector connector = new KafkaConnector();
        await using var sink = await connector.OpenAsync(new ConnectorConfig(_broker.ConnectionConfig()), CancellationToken.None);

        var acc = new ConnectorStatsAccumulator();
        using var _ = ConnectorStats.Use(acc);
        await using var session = await sink.BeginWriteAsync(
            new OutputSpec("kafka", topic, "append", "fail_on_change", new Dictionary<string, object?>()), schema, CancellationToken.None);
        using (var batch = new RecordBatch(schema, [new Int64Array.Builder().Append(1).Append(2).Build()], 2))
        {
            await session.WriteBatchAsync(batch, CancellationToken.None);
        }

        await session.CommitAsync(CancellationToken.None);

        // Read before the session is disposed: the engine snapshots at commit.
        var s = acc.Snapshot();
        Assert.True(s.Requests > 0);
        Assert.Contains(new KeyValuePair<string, double>("kafka.messages", 2), s.Extras);
    }

    [SkippableFact]
    public async Task Real_statistics_ticks_reach_the_handle_captured_at_begin()
    {
        var topic = await _broker.CreateTopicAsync(partitions: 1);
        var schema = new Schema([new Field("id", Int64Type.Default, true)], null);
        ISinkConnector connector = new KafkaConnector();
        await using var sink = await connector.OpenAsync(new ConnectorConfig(_broker.ConnectionConfig()), CancellationToken.None);

        var acc = new ConnectorStatsAccumulator();
        ISinkWriteSession session;
        using (ConnectorStats.Use(acc))
        {
            session = await sink.BeginWriteAsync(
                new OutputSpec("kafka", topic, "append", "fail_on_change", new Dictionary<string, object?>()), schema, CancellationToken.None);
        }

        // The ambient handle is gone by the time librdkafka's thread reports; only the captured one can receive it.
        await using (session)
        {
            using var batch = new RecordBatch(schema, [new Int64Array.Builder().Append(1).Build()], 1);
            await session.WriteBatchAsync(batch, CancellationToken.None);

            // No commit yet, so the close-time floor cannot be what is observed.
            using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            while (acc.Snapshot().Requests == 0)
            {
                await Task.Delay(50, deadline.Token);
            }

            Assert.True(acc.Snapshot().WireBytes > 0);
        }
    }
}
