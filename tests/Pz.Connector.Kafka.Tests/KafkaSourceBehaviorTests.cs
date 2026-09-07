using System.Text;
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

    [SkippableFact]
    public async Task Second_run_with_the_token_lands_only_records_produced_since()
    {
        var topic = await _broker.CreateTopicAsync(partitions: 2);
        await _broker.ProduceTextAsync(topic, ["a", "b"], partition: 0);
        var (_, token) = await ReadAsync(new KafkaConnector(), Config, Spec(topic));
        await _broker.ProduceTextAsync(topic, ["c"], partition: 0);
        await _broker.ProduceTextAsync(topic, ["d"], partition: 1);

        var (rows, token2) = await ReadAsync(new KafkaConnector(), Config, Spec(topic, token));

        Assert.Equal(["c", "d"], rows.OrderBy(r => r.Partition).Select(r => r.Value));
        Assert.Equal($$$"""{"v":1,"topic":"{{{topic}}}","partitions":{"0":3,"1":1}}""", token2);
    }

    [SkippableFact]
    public async Task Zero_row_run_still_emits_a_token()
    {
        var topic = await _broker.CreateTopicAsync(partitions: 1);

        var (rows, token) = await ReadAsync(new KafkaConnector(), Config, Spec(topic));

        Assert.Empty(rows);
        Assert.Equal($$$"""{"v":1,"topic":"{{{topic}}}","partitions":{"0":0}}""", token);
    }

    [SkippableFact]
    public async Task Start_latest_lands_nothing_and_records_the_bound()
    {
        var topic = await _broker.CreateTopicAsync(partitions: 1);
        await _broker.ProduceTextAsync(topic, ["old1", "old2"]);

        var (rows, token) = await ReadAsync(new KafkaConnector(), Config, Spec(topic, options: new() { ["start"] = "latest" }));

        Assert.Empty(rows);
        Assert.Equal($$$"""{"v":1,"topic":"{{{topic}}}","partitions":{"0":2}}""", token);
    }

    [SkippableFact]
    public async Task Start_timestamp_skips_older_records()
    {
        var topic = await _broker.CreateTopicAsync(partitions: 1);
        await _broker.ProduceTextAsync(topic, ["old"]);
        var all = await _broker.ConsumeAllAsync(topic);
        var cut = all[0].Message.Timestamp.UtcDateTime.AddMilliseconds(1);
        // Wait until the clock has moved past the cut before producing the newer record.
        while (DateTime.UtcNow <= cut)
        {
            await Task.Yield();
        }

        await _broker.ProduceTextAsync(topic, ["new"]);

        var (rows, _) = await ReadAsync(new KafkaConnector(), Config,
            Spec(topic, options: new() { ["start"] = cut.ToString("o") }));

        Assert.Equal(["new"], rows.Select(r => r.Value));
    }

    [SkippableFact]
    public async Task Records_produced_past_the_bound_during_a_read_are_not_landed()
    {
        var topic = await _broker.CreateTopicAsync(partitions: 1);
        await _broker.ProduceTextAsync(topic, Enumerable.Range(0, 50).Select(i => $"r{i}"));
        ISourceConnector connector = new KafkaConnector();
        await using var source = await connector.OpenAsync(Config, CancellationToken.None);
        var partition = Assert.Single(await source.PlanReadAsync(Spec(topic), ReadHints.None, CancellationToken.None));

        var rows = new List<string?>();
        var produced = false;
        await foreach (var batch in partition.ReadAsync(new BatchOptions(TargetBatchBytes: 512, MaxRowsPerBatch: 10), CancellationToken.None))
        {
            using (batch)
            {
                var v = (StringArray)batch.Column(5);
                for (var i = 0; i < batch.Length; i++)
                {
                    rows.Add(v.GetString(i));
                }
            }

            if (!produced)
            {
                produced = true;
                await _broker.ProduceTextAsync(topic, ["late"]);
            }
        }

        Assert.Equal(50, rows.Count);
        Assert.DoesNotContain("late", rows);
        Assert.True(((ISyncStatePartition)partition).TryGetSyncStateCandidate(out var token));
        Assert.Equal($$$"""{"v":1,"topic":"{{{topic}}}","partitions":{"0":50}}""", token);
    }

    [SkippableFact]
    public async Task Partition_added_between_runs_starts_at_start()
    {
        var topic = await _broker.CreateTopicAsync(partitions: 1);
        await _broker.ProduceTextAsync(topic, ["a"]);
        var (_, token) = await ReadAsync(new KafkaConnector(), Config, Spec(topic));
        await _broker.AddPartitionsAsync(topic, total: 2);
        await _broker.ProduceTextAsync(topic, ["p1-a", "p1-b"], partition: 1);

        var (rows, token2) = await ReadAsync(new KafkaConnector(), Config, Spec(topic, token));

        Assert.Equal(["p1-a", "p1-b"], rows.Select(r => r.Value));
        Assert.Equal($$$"""{"v":1,"topic":"{{{topic}}}","partitions":{"0":1,"1":2}}""", token2);
    }

    [SkippableFact]
    public async Task Token_below_the_low_watermark_fails_non_transiently_naming_retention()
    {
        var topic = await _broker.CreateTopicAsync(partitions: 1);
        await _broker.ProduceTextAsync(topic, ["a", "b", "c"]);
        // The stored token says "resume at 1"; the broker has since dropped offsets 0 and 1.
        var token = new OffsetToken(topic, new Dictionary<int, long> { [0] = 1 }).Serialize();
        await _broker.DeleteRecordsAsync(topic, partition: 0, beforeOffset: 2);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => ReadAsync(new KafkaConnector(), Config, Spec(topic, token)));

        Assert.False(ex.IsTransient);
        Assert.Contains("partition 0", ex.Message);
        Assert.Contains("dropped by retention", ex.Message);
        Assert.Contains("--full-refresh", ex.Message);
    }

    [SkippableFact]
    public async Task Token_beyond_the_high_watermark_fails_non_transiently()
    {
        var topic = await _broker.CreateTopicAsync(partitions: 1);
        await _broker.ProduceTextAsync(topic, ["a"]);
        var stale = new OffsetToken(topic, new Dictionary<int, long> { [0] = 40 }).Serialize();

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => ReadAsync(new KafkaConnector(), Config, Spec(topic, stale)));

        Assert.False(ex.IsTransient);
        Assert.Contains("partition 0", ex.Message);
        Assert.Contains("--full-refresh", ex.Message);
    }

    [SkippableFact]
    public async Task Token_for_another_topic_is_refused()
    {
        var topic = await _broker.CreateTopicAsync(partitions: 1);
        var foreign = new OffsetToken("someone-elses-topic", new Dictionary<int, long> { [0] = 0 }).Serialize();

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => ReadAsync(new KafkaConnector(), Config, Spec(topic, foreign)));

        Assert.False(ex.IsTransient);
        Assert.Contains("someone-elses-topic", ex.Message);
    }

    [SkippableFact]
    public async Task Unknown_topic_fails_non_transiently_naming_it()
    {
        var ex = await Assert.ThrowsAsync<PzConnectorException>(() =>
            ReadAsync(new KafkaConnector(), Config, Spec("pz_does_not_exist_" + Guid.NewGuid().ToString("N"))));

        Assert.False(ex.IsTransient);
        Assert.Contains("does not exist", ex.Message);
    }

    [SkippableFact]
    public async Task Cancelled_read_emits_no_candidate()
    {
        var topic = await _broker.CreateTopicAsync(partitions: 1);
        await _broker.ProduceTextAsync(topic, Enumerable.Range(0, 5_000).Select(i => $"{{\"i\":{i}}}"));
        ISourceConnector connector = new KafkaConnector();
        await using var source = await connector.OpenAsync(Config, CancellationToken.None);
        var partition = Assert.Single(await source.PlanReadAsync(Spec(topic), ReadHints.None, CancellationToken.None));
        using var cts = new CancellationTokenSource();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await foreach (var batch in partition.ReadAsync(new BatchOptions(TargetBatchBytes: 1024, MaxRowsPerBatch: 50), cts.Token))
            {
                batch.Dispose();
                cts.Cancel();
            }
        });

        Assert.False(((ISyncStatePartition)partition).TryGetSyncStateCandidate(out var token));
        Assert.Null(token);
    }

    [SkippableFact]
    public async Task Envelope_carries_key_headers_and_a_utc_timestamp()
    {
        var topic = await _broker.CreateTopicAsync(partitions: 1);
        var before = DateTimeOffset.UtcNow.AddSeconds(-5);
        await _broker.ProduceAsync(topic, [(0, Encoding.UTF8.GetBytes("k"), Encoding.UTF8.GetBytes("v"),
            new List<KeyValuePair<string, byte[]?>> { new("h1", Encoding.UTF8.GetBytes("x")), new("h1", Encoding.UTF8.GetBytes("y")) })]);
        ISourceConnector connector = new KafkaConnector();
        await using var source = await connector.OpenAsync(Config, CancellationToken.None);
        var partition = Assert.Single(await source.PlanReadAsync(Spec(topic), ReadHints.None, CancellationToken.None));

        RecordBatch? only = null;
        await foreach (var batch in partition.ReadAsync(BatchOptions.Default, CancellationToken.None))
        {
            Assert.Null(only);
            only = batch;
        }

        Assert.NotNull(only);
        using (only)
        {
            Assert.Equal(topic, ((StringArray)only.Column(0)).GetString(0));
            Assert.Equal("k", ((StringArray)only.Column(4)).GetString(0));
            Assert.Equal("v", ((StringArray)only.Column(5)).GetString(0));
            Assert.Equal("{\"h1\":[\"x\",\"y\"]}", ((StringArray)only.Column(6)).GetString(0));
            var ts = ((TimestampArray)only.Column(3)).GetTimestamp(0)!.Value;
            Assert.InRange(ts, before, DateTimeOffset.UtcNow.AddSeconds(5));
        }
    }

    [SkippableFact]
    public async Task Check_connection_reports_brokers_or_a_redacted_failure()
    {
        var ok = await new KafkaConnector().CheckConnectionAsync(Config, CancellationToken.None);
        Assert.True(ok.Ok);
        Assert.Contains("broker", ok.Message);

        var bad = await new KafkaConnector().CheckConnectionAsync(new ConnectorConfig(new Dictionary<string, object?>
        {
            ["bootstrap_servers"] = "127.0.0.1:1",
            ["client"] = new Dictionary<string, object?> { ["socket.timeout.ms"] = 1000L, ["metadata.request.timeout.ms"] = 1000L },
        }), CancellationToken.None);
        Assert.False(bad.Ok);
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
