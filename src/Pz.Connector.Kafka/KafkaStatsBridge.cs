using System.Text.Json;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Kafka;

/// <summary>Turns one librdkafka client's cumulative statistics JSON into deltas on a stats handle.
/// The handle is captured when the operation starts and handed in: statistics arrive on
/// librdkafka's own thread, where the ambient <see cref="ConnectorStats.Current"/> does not flow,
/// so nothing here ever reads it.
///
/// <para>A client that closes before its first statistics tick (a short read, a small write) has
/// reported nothing, yet it did issue at least one fetch or produce request. <see cref="Complete"/>
/// adds that one request as the floor, and only when no tick ever arrived; real ticks supersede it.</para></summary>
internal sealed class KafkaStatsBridge(IConnectorStats target) : IDisposable
{
    private readonly object _gate = new();
    private long _tx;
    private long _rx;
    private long _txBytes;
    private long _rxBytes;
    private long _throttle;
    private bool _ticked;
    private bool _completed;

    /// <summary>Called on librdkafka's statistics thread with the cumulative JSON; adds the delta since the last call.</summary>
    public void OnStatistics(string json)
    {
        long tx, rx, txBytes, rxBytes, throttle;
        try
        {
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;
            tx = Counter(root, "tx");
            rx = Counter(root, "rx");
            txBytes = Counter(root, "tx_bytes");
            rxBytes = Counter(root, "rx_bytes");
            throttle = 0;
            if (root.TryGetProperty("brokers", out var brokers) && brokers.ValueKind == JsonValueKind.Object)
            {
                foreach (var broker in brokers.EnumerateObject())
                {
                    if (broker.Value.ValueKind == JsonValueKind.Object
                        && broker.Value.TryGetProperty("throttle", out var t) && t.ValueKind == JsonValueKind.Object)
                    {
                        throttle += Counter(t, "cnt");
                    }
                }
            }
        }
        catch (JsonException)
        {
            return;
        }

        long dRequests, dBytes, dThrottle;
        lock (_gate)
        {
            _ticked = true;
            dRequests = Delta(tx, ref _tx) + Delta(rx, ref _rx);
            dBytes = Delta(txBytes, ref _txBytes) + Delta(rxBytes, ref _rxBytes);
            dThrottle = Delta(throttle, ref _throttle);
        }

        target.AddRequests(dRequests);
        target.AddWireBytes(dBytes);
        target.AddThrottled(dThrottle);
    }

    /// <summary>Counts messages the connector itself consumed or produced, for clients that close before any statistics tick.</summary>
    public void CountMessages(long n) => target.AddExtra("kafka.messages", n);

    /// <summary>Declared before the client it serves, so a using scope closes the client first and
    /// then completes the bridge.</summary>
    public void Dispose() => Complete();

    /// <summary>Call once the client is closed; adds the one-request floor if no tick ever arrived.</summary>
    public void Complete()
    {
        lock (_gate)
        {
            if (_completed)
            {
                return;
            }

            _completed = true;
            if (_ticked)
            {
                return;
            }
        }

        target.AddRequests(1);
    }

    private static long Counter(JsonElement element, string name) =>
        element.TryGetProperty(name, out var v) && v.ValueKind == JsonValueKind.Number && v.TryGetInt64(out var n) ? n : 0;

    private static long Delta(long current, ref long previous)
    {
        var delta = Math.Max(0, current - previous);
        previous = Math.Max(previous, current);
        return delta;
    }
}
