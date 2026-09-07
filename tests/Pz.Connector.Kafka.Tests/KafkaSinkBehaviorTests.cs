using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Kafka.Tests;

/// <summary>What the TestKit contract cannot express: the record shape each option combination
/// produces, and the two failure paths a real broker owns (a topic that does not exist, and an
/// abort over records already handed to the producer).</summary>
[Collection("kafka")]
[Trait("Category", "Docker")]
public sealed class KafkaSinkBehaviorTests
{
    private static readonly Schema Schema = new(
    [
        new Field("id", Int64Type.Default, true),
        new Field("name", StringType.Default, true),
        new Field("src", StringType.Default, true),
        new Field("when", new TimestampType(TimeUnit.Microsecond, "UTC"), true),
        new Field("payload", StringType.Default, true),
    ], null);

    private readonly KafkaBrokerFixture _broker;

    public KafkaSinkBehaviorTests(KafkaBrokerFixture broker)
    {
        _broker = broker;
        DockerFacts.SkipUnlessDocker();
    }

    private ConnectorConfig Config => new(_broker.ConnectionConfig());

    private static OutputSpec Spec(string topic, Dictionary<string, object?>? options = null) =>
        new("kafka", topic, "append", "fail_on_change", options ?? []);

    private static RecordBatch Batch()
    {
        var ts = new DateTimeOffset(2026, 9, 7, 1, 2, 3, TimeSpan.Zero);
        return new RecordBatch(Schema,
        [
            new Int64Array.Builder().Append(1).Append(2).Build(),
            new StringArray.Builder().Append("ann").AppendNull().Build(),
            new StringArray.Builder().Append("web").Append("app").Build(),
            new TimestampArray.Builder(new TimestampType(TimeUnit.Microsecond, "UTC")).Append(ts).Append(ts).Build(),
            new StringArray.Builder().Append("{\"raw\":1}").Append("{\"raw\":2}").Build(),
        ], 2);
    }

    private async Task<WriteResult> WriteAsync(OutputSpec spec, params RecordBatch[] batches)
    {
        var connector = new KafkaConnector();
        await using var sink = await ((ISinkConnector)connector).OpenAsync(Config, CancellationToken.None);
        await using var session = await sink.BeginWriteAsync(spec, Schema, CancellationToken.None);
        foreach (var batch in batches)
        {
            await session.WriteBatchAsync(batch, CancellationToken.None);
            batch.Dispose();
        }

        return await session.CommitAsync(CancellationToken.None);
    }

    [SkippableFact]
    public async Task Whole_row_json_is_the_default_value_with_null_key()
    {
        var topic = await _broker.CreateTopicAsync(partitions: 1);

        var result = await WriteAsync(Spec(topic), Batch());

        Assert.Equal(2, result.RowsWritten);
        Assert.Equal(1, result.BatchesWritten);
        var records = await _broker.ConsumeAllAsync(topic);
        Assert.Equal(2, records.Count);
        Assert.Null(records[0].Message.Key);
        Assert.Equal("""{"id":1,"name":"ann","src":"web","when":"2026-09-07T01:02:03.000000Z","payload":"{\"raw\":1}"}""",
            Encoding.UTF8.GetString(records[0].Message.Value));
        Assert.Equal("""{"id":2,"name":null,"src":"app","when":"2026-09-07T01:02:03.000000Z","payload":"{\"raw\":2}"}""",
            Encoding.UTF8.GetString(records[1].Message.Value));
        Assert.True(records[0].Message.Headers is null || !records[0].Message.Headers.Any());
    }

    [SkippableFact]
    public async Task Key_and_header_columns_are_sent_as_such_and_excluded_from_the_value()
    {
        var topic = await _broker.CreateTopicAsync(partitions: 1);

        await WriteAsync(Spec(topic, new() { ["key"] = "id", ["headers"] = new List<object?> { "src", "name" } }), Batch());

        var records = await _broker.ConsumeAllAsync(topic);
        Assert.Equal("1", Encoding.UTF8.GetString(records[0].Message.Key));
        Assert.Equal("web", Encoding.UTF8.GetString(records[0].Message.Headers.GetLastBytes("src")));
        Assert.Equal("ann", Encoding.UTF8.GetString(records[0].Message.Headers.GetLastBytes("name")));
        Assert.Equal("""{"when":"2026-09-07T01:02:03.000000Z","payload":"{\"raw\":1}"}""", Encoding.UTF8.GetString(records[0].Message.Value));
        // A null header value omits the header.
        Assert.False(records[1].Message.Headers.TryGetLastBytes("name", out _));
        Assert.Equal("2", Encoding.UTF8.GetString(records[1].Message.Key));
    }

    [SkippableFact]
    public async Task Explicit_value_column_is_sent_verbatim()
    {
        var topic = await _broker.CreateTopicAsync(partitions: 1);

        await WriteAsync(Spec(topic, new() { ["value"] = "payload", ["key"] = "id" }), Batch());

        var records = await _broker.ConsumeAllAsync(topic);
        Assert.Equal("{\"raw\":1}", Encoding.UTF8.GetString(records[0].Message.Value));
        Assert.Equal("{\"raw\":2}", Encoding.UTF8.GetString(records[1].Message.Value));
    }

    [SkippableFact]
    public async Task Compression_round_trips()
    {
        var topic = await _broker.CreateTopicAsync(partitions: 1);

        await WriteAsync(Spec(topic, new() { ["compression"] = "zstd" }), Batch());

        Assert.Equal(2, (await _broker.ConsumeAllAsync(topic)).Count);
    }

    [SkippableFact]
    public async Task Rows_write_to_the_explicit_topic_option()
    {
        var topic = await _broker.CreateTopicAsync(partitions: 1);

        await WriteAsync(new OutputSpec("kafka", "some_entity", "append", "fail_on_change",
            new Dictionary<string, object?> { ["topic"] = topic }), Batch());

        Assert.Equal(2, (await _broker.ConsumeAllAsync(topic)).Count);
    }

    [SkippableFact]
    public async Task Begin_write_refuses_columns_the_schema_lacks()
    {
        var topic = await _broker.CreateTopicAsync(partitions: 1);
        var connector = new KafkaConnector();
        await using var sink = await ((ISinkConnector)connector).OpenAsync(Config, CancellationToken.None);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
            await sink.BeginWriteAsync(Spec(topic, new() { ["key"] = "missing" }), Schema, CancellationToken.None));

        Assert.False(ex.IsTransient);
        Assert.Contains("'key'", ex.Message);
        Assert.Contains("missing", ex.Message);
    }

    [SkippableFact]
    public async Task Unknown_topic_fails_commit_non_transiently()
    {
        var connector = new KafkaConnector();
        await using var sink = await ((ISinkConnector)connector).OpenAsync(new ConnectorConfig(new Dictionary<string, object?>
        {
            ["bootstrap_servers"] = _broker.BootstrapServers,
            // Generous enough that Local_UnknownTopic (non-transient) is always the failure the
            // commit reports, never a message timeout (transient) racing it.
            ["client"] = new Dictionary<string, object?> { ["allow.auto.create.topics"] = false, ["message.timeout.ms"] = 30000L },
        }), CancellationToken.None);
        var missing = "pz_missing_" + Guid.NewGuid().ToString("N");
        await using var session = await sink.BeginWriteAsync(Spec(missing), Schema, CancellationToken.None);
        using (var batch = Batch())
        {
            await session.WriteBatchAsync(batch, CancellationToken.None);
        }

        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () => await session.CommitAsync(CancellationToken.None));

        Assert.False(ex.IsTransient);
        Assert.Contains(missing, ex.Message);
    }

    [SkippableFact]
    public async Task Abort_after_a_write_succeeds_and_forbids_commit()
    {
        var topic = await _broker.CreateTopicAsync(partitions: 1);
        var connector = new KafkaConnector();
        await using var sink = await ((ISinkConnector)connector).OpenAsync(Config, CancellationToken.None);
        Assert.Equal(AbortSemantics.None, sink.AbortSemantics);
        await using var session = await sink.BeginWriteAsync(Spec(topic), Schema, CancellationToken.None);
        using (var batch = Batch())
        {
            await session.WriteBatchAsync(batch, CancellationToken.None);
        }

        await session.AbortAsync(CancellationToken.None);

        await Assert.ThrowsAsync<InvalidOperationException>(async () => await session.CommitAsync(CancellationToken.None));
    }
}
