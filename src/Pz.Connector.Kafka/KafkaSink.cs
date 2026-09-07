using System.Diagnostics.CodeAnalysis;
using Apache.Arrow;
using Microsoft.Extensions.Logging;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Kafka;

/// <summary>Append-only produce. No native copy (DuckDB cannot speak the Kafka protocol) and
/// <see cref="AbortSemantics.None"/>: a produced record cannot be unsent, so abort only stops
/// further deliveries.</summary>
internal sealed class KafkaSink(KafkaConnectionConfig connection, IKafkaClientFactory factory, ILogger logger) : ISink
{
    public AbortSemantics AbortSemantics => AbortSemantics.None;

    public bool TryGetNativeCopy(OutputSpec spec, [NotNullWhen(true)] out NativeCopy? copy)
    {
        copy = null;
        return false;
    }

    public ValueTask<ISinkWriteSession> BeginWriteAsync(OutputSpec spec, Schema schema, CancellationToken ct)
    {
        var errors = new List<string>();
        var output = KafkaOutputConfig.Parse(spec, errors);
        output?.ValidateAgainst(spec.Output, schema, errors);
        if (output is null || errors.Count > 0)
        {
            // Parse and ValidateAgainst already name the output in every message they add.
            throw KafkaErrors.Fatal(string.Join("; ", errors), connection.Redactor);
        }

        if (!string.Equals(spec.Mode, "append", StringComparison.Ordinal))
        {
            throw KafkaErrors.Fatal($"output '{spec.Output}': mode '{spec.Mode}' is not supported; kafka is append-only", connection.Redactor);
        }

        var producer = factory.CreateProducer(connection.ClientProperties, output.Compression);
        return ValueTask.FromResult<ISinkWriteSession>(new KafkaWriteSession(
            producer, output, schema, connection.Redactor, logger, TimeSpan.FromSeconds(connection.IdleTimeoutSeconds)));
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
