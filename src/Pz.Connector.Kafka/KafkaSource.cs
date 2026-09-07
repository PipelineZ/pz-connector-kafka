using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.Logging;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Kafka;

/// <summary>Every dataset is feed-shaped (<see cref="NaturalReadShape.Feed"/>): the connector owns
/// the resume position. There is no native scan -- the token only exists once the partition has
/// been drained over the data plane -- and exactly one pz partition, because the engine captures
/// one sync-state candidate per dataset.</summary>
internal sealed class KafkaSource(KafkaConnectionConfig connection, IKafkaClientFactory factory, ILogger logger)
    : ISource, INaturalReadShapeSource
{
    public NaturalReadShape GetNaturalReadShape(DatasetSpec spec) => NaturalReadShape.Feed;

    public ValueTask<DatasetSchema> GetSchemaAsync(DatasetSpec spec, CancellationToken ct)
    {
        ParseDataset(spec);
        return ValueTask.FromResult(new DatasetSchema(EnvelopeBatchBuilder.Schema));
    }

    public bool TryGetNativeScan(DatasetSpec spec, [NotNullWhen(true)] out NativeScan? scan)
    {
        scan = null;
        return false;
    }

    public ValueTask<IReadOnlyList<IDatasetPartition>> PlanReadAsync(DatasetSpec spec, ReadHints hints, CancellationToken ct)
    {
        var dataset = ParseDataset(spec);
        var token = spec.PriorSyncState is { Length: > 0 } prior
            ? OffsetToken.Parse(prior, dataset.Topic, connection.Redactor)
            : null;
        IReadOnlyList<IDatasetPartition> partitions = [new KafkaPartition(connection, factory, dataset, token, logger)];
        return ValueTask.FromResult(partitions);
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private KafkaDatasetConfig ParseDataset(DatasetSpec spec)
    {
        var errors = new List<string>();
        return KafkaDatasetConfig.Parse(spec, errors)
            ?? throw KafkaErrors.Fatal(string.Join("; ", errors), connection.Redactor);
    }
}
