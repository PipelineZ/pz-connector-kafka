using Apache.Arrow;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Kafka;

/// <summary>One idempotent producer per session. Every row's key/value/headers are fresh byte
/// arrays built before Produce returns, so nothing from the engine-owned batch outlives the call.
/// Back-pressure: a full local queue polls delivery reports and retries the same row -- rows are
/// never dropped and the queue never grows past librdkafka's own bound. The first failed delivery
/// report is remembered and thrown by the next WriteBatchAsync or by CommitAsync, whichever comes
/// first; Commit also fails when Flush leaves anything outstanding.
///
/// <para>Commit flushes in slices so cancellation stays responsive, and gives up once the out-queue
/// has failed to shrink for the connection's idle timeout -- the same "nothing has happened for this
/// long" bound the source applies to a silent broker. Without it the loop's only bound would be
/// librdkafka's own message.timeout.ms, which classifies the outcome as a message timeout rather
/// than as the transient stall it is.</para></summary>
internal sealed class KafkaWriteSession : ISinkWriteSession
{
    private static readonly TimeSpan QueueFullBackoff = TimeSpan.FromMilliseconds(100);
    private static readonly TimeSpan FlushSlice = TimeSpan.FromMilliseconds(250);

    private readonly IProducer<byte[], byte[]> _producer;
    private readonly KafkaOutputConfig _output;
    private readonly KafkaRedactor _redactor;
    private readonly ILogger _logger;
    private readonly RowJsonWriter? _json;
    private readonly int _keyIndex;
    private readonly int _valueIndex;
    private readonly int[] _headerIndexes;
    private readonly Action<DeliveryReport<byte[], byte[]>> _onDelivery;
    private readonly int _flushStallSlices;
    private Error? _firstFailure;
    private long _rows;
    private long _batches;
    private bool _committed;
    private bool _aborted;
    private bool _disposed;

    public KafkaWriteSession(IProducer<byte[], byte[]> producer, KafkaOutputConfig output, Schema schema,
        KafkaRedactor redactor, ILogger logger, TimeSpan? flushStall = null)
    {
        _producer = producer;
        _output = output;
        _redactor = redactor;
        _logger = logger;
        _json = output.ValueColumn is null ? new RowJsonWriter(schema, output.JsonColumnIndexes(schema)) : null;
        _keyIndex = IndexOf(schema, output.KeyColumn);
        _valueIndex = IndexOf(schema, output.ValueColumn);
        _headerIndexes = _output.HeaderColumns.Select(h => IndexOf(schema, h)).ToArray();
        // At least one slice. The first flush only establishes the baseline the next one is
        // compared against, so a zero budget gives up on the first flush that fails to shrink the
        // queue -- the second flush overall.
        _flushStallSlices = Math.Max(1, (int)((flushStall ?? TimeSpan.FromSeconds(60)).Ticks / FlushSlice.Ticks));
        // Delivery reports arrive on the producer's own poll thread, not on the thread that
        // produced: the first failure is claimed under an interlocked write and read back with a
        // matching volatile read.
        _onDelivery = report =>
        {
            if (report.Error.IsError)
            {
                Interlocked.CompareExchange(ref _firstFailure, report.Error, null);
            }
        };
    }

    public ValueTask WriteBatchAsync(RecordBatch batch, CancellationToken ct)
    {
        ThrowIfFinished();
        ThrowIfDeliveryFailed();
        for (var row = 0; row < batch.Length; row++)
        {
            ct.ThrowIfCancellationRequested();
            var message = new Message<byte[], byte[]>
            {
                Key = _keyIndex >= 0 ? ArrowScalars.Bytes(batch.Column(_keyIndex), row)! : null!,
                Value = _valueIndex >= 0 ? ArrowScalars.Bytes(batch.Column(_valueIndex), row)! : _json!.Write(batch, row),
            };
            if (_headerIndexes.Length > 0)
            {
                message.Headers = new Headers();
                for (var h = 0; h < _headerIndexes.Length; h++)
                {
                    if (ArrowScalars.Bytes(batch.Column(_headerIndexes[h]), row) is { } bytes)
                    {
                        message.Headers.Add(_output.HeaderColumns[h], bytes);
                    }
                }
            }

            ProduceWithBackpressure(message, ct);
            _rows++;
        }

        _batches++;
        return ValueTask.CompletedTask;
    }

    public ValueTask<WriteResult> CommitAsync(CancellationToken ct)
    {
        ThrowIfFinished();
        _committed = true;
        int outstanding;
        var previous = int.MaxValue;
        var stalled = 0;
        do
        {
            ct.ThrowIfCancellationRequested();
            outstanding = _producer.Flush(FlushSlice);
            stalled = outstanding < previous ? 0 : stalled + 1;
            previous = outstanding;
        }
        while (outstanding > 0 && Volatile.Read(ref _firstFailure) is null && stalled < _flushStallSlices);

        ThrowIfDeliveryFailed();
        if (outstanding > 0)
        {
            throw KafkaErrors.Transient($"topic '{_output.Topic}': {outstanding} record(s) still unacknowledged after flush", _redactor);
        }

        _logger.LogDebug("kafka: topic {Topic}: committed {Rows} rows in {Batches} batches", _output.Topic, _rows, _batches);
        return ValueTask.FromResult(new WriteResult(_rows, _batches));
    }

    public ValueTask AbortAsync(CancellationToken ct)
    {
        if (_committed)
        {
            throw new InvalidOperationException("AbortAsync after CommitAsync is not allowed");
        }

        _aborted = true;
        // Nothing to unsend: a record handed to the producer is already on its way, which is what
        // AbortSemantics.None declares. One bounded flush lets the reports for what is already
        // queued land here rather than inside Dispose, and the session then refuses further work.
        _producer.Flush(FlushSlice);
        return ValueTask.CompletedTask;
    }

    public ValueTask DisposeAsync()
    {
        if (!_disposed)
        {
            _disposed = true;
            _producer.Dispose();
        }

        return ValueTask.CompletedTask;
    }

    private void ProduceWithBackpressure(Message<byte[], byte[]> message, CancellationToken ct)
    {
        while (true)
        {
            try
            {
                _producer.Produce(_output.Topic, message, _onDelivery);
                return;
            }
            catch (ProduceException<byte[], byte[]> ex) when (ex.Error.Code == ErrorCode.Local_QueueFull)
            {
                _producer.Poll(QueueFullBackoff);
                ThrowIfDeliveryFailed();
                ct.ThrowIfCancellationRequested();
            }
            catch (ProduceException<byte[], byte[]> ex)
            {
                throw KafkaErrors.Wrap(ex, _redactor, $"topic '{_output.Topic}': producing");
            }
        }
    }

    private void ThrowIfDeliveryFailed()
    {
        if (Volatile.Read(ref _firstFailure) is { } error)
        {
            throw new PzConnectorException(
                _redactor.Redact($"kafka: topic '{_output.Topic}': delivery failed: {error.Code} -- {error.Reason}" +
                                (error.Code is ErrorCode.UnknownTopicOrPart or ErrorCode.Local_UnknownTopic
                                    ? " (create the topic; auto-create depends on broker policy)"
                                    : "")),
                KafkaErrors.IsTransient(error.Code));
        }
    }

    private void ThrowIfFinished()
    {
        if (_committed)
        {
            throw new InvalidOperationException("the session is already committed");
        }

        if (_aborted)
        {
            throw new InvalidOperationException("the session is aborted");
        }
    }

    private static int IndexOf(Schema schema, string? column) =>
        column is null ? -1 : schema.FieldsList.ToList().FindIndex(f => f.Name == column);
}
