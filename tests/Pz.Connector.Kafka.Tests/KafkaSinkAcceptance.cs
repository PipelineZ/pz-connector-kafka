using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit;

namespace Pz.Connector.Kafka.Tests;

/// <summary>TestKit sink contract. The suite writes a fixed (id Int64, name String) schema; the
/// connector produces each row as {"id":..,"name":..}, so read-back parses the JSON values into the
/// same two columns. One fresh topic per test-class instance (xunit instantiates per fact), so
/// facts never see each other's records. No Merge/Replace/Checkpoint outputs: none is declared.</summary>
[Collection("kafka")]
[Trait("Category", "Docker")]
public sealed class KafkaSinkAcceptance : SinkConnectorAcceptanceTests
{
    private readonly KafkaBrokerFixture _broker;
    private readonly string _topic;

    public KafkaSinkAcceptance(KafkaBrokerFixture broker)
    {
        _broker = broker;
        DockerFacts.SkipUnlessDocker();
        _topic = broker.CreateTopicAsync(partitions: 2).GetAwaiter().GetResult();
    }

    protected override void GateFact() => DockerFacts.SkipUnlessDocker();

    protected override ISinkConnector CreateSink() => new KafkaConnector();

    protected override ConnectorConfig ValidConfig => new(_broker.ConnectionConfig());

    protected override OutputSpec SmallOutput => new("kafka", _topic, "append", "fail_on_change", new Dictionary<string, object?>());

    protected override async ValueTask<IReadOnlyList<RecordBatch>> ReadCommittedAsync(ISinkConnector connector, OutputSpec spec)
    {
        var records = await _broker.ConsumeAllAsync(spec.Output);
        if (records.Count == 0)
        {
            return [];
        }

        var ids = new Int64Array.Builder();
        var names = new StringArray.Builder();
        foreach (var record in records)
        {
            using var doc = JsonDocument.Parse(record.Message.Value);
            ids.Append(doc.RootElement.GetProperty("id").GetInt64());
            names.Append(doc.RootElement.GetProperty("name").GetString()!);
        }

        var schema = new Schema([new Field("id", Int64Type.Default, false), new Field("name", StringType.Default, false)], null);
        return [new RecordBatch(schema, [ids.Build(), names.Build()], records.Count)];
    }
}
