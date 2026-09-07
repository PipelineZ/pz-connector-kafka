using System.Diagnostics.CodeAnalysis;
using Apache.Arrow;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Kafka;

/// <summary>Append-only produce. No native copy (DuckDB cannot speak the Kafka protocol) and
/// <see cref="AbortSemantics.None"/>: a produced record cannot be unsent, so abort refuses further
/// writes and flushes what is already queued -- an aborted session leaves nothing it accepted
/// unsent, it only stops the run from handing over more.</summary>
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
        if (!string.Equals(spec.Mode, "append", StringComparison.Ordinal))
        {
            errors.Add($"output '{spec.Output}': mode '{spec.Mode}' is not supported; kafka is append-only");
        }

        if (output is null || errors.Count > 0)
        {
            // Parse and ValidateAgainst already name the output in every message they add.
            throw KafkaErrors.Fatal(string.Join("; ", errors), connection.Redactor);
        }

        IProducer<byte[], byte[]> producer;
        try
        {
            producer = factory.CreateProducer(connection.ClientProperties, output.Compression);
        }
        catch (Exception ex)
        {
            // librdkafka rejects a `client:` property inside the builder, quoting the value it
            // refused; outside the try below because there is no handle to dispose yet.
            throw KafkaErrors.Wrap(ex, connection.Redactor,
                $"topic '{output.Topic}': building the kafka client; check `client:` properties");
        }

        try
        {
            return ValueTask.FromResult<ISinkWriteSession>(new KafkaWriteSession(
                producer, output, schema, connection.Redactor, logger, TimeSpan.FromSeconds(connection.IdleTimeoutSeconds)));
        }
        catch
        {
            // Nothing owns the librdkafka handle until the session exists.
            producer.Dispose();
            throw;
        }
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
