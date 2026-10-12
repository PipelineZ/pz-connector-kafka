using Pz.Connectors.Abstractions;

namespace Pz.Connector.Kafka.Tests;

public sealed class KafkaStatsBridgeTests
{
    private const string First = "{\"tx\":4,\"rx\":6,\"tx_bytes\":400,\"rx_bytes\":600,\"brokers\":{\"b1\":{\"throttle\":{\"cnt\":0}}}}";
    private const string Second = "{\"tx\":10,\"rx\":9,\"tx_bytes\":1000,\"rx_bytes\":900,\"brokers\":{\"b1\":{\"throttle\":{\"cnt\":2}}}}";

    [Fact]
    public void Cumulative_statistics_are_added_as_deltas()
    {
        var acc = new ConnectorStatsAccumulator();
        var bridge = new KafkaStatsBridge(acc);
        bridge.OnStatistics(First);
        bridge.OnStatistics(Second);
        var s = acc.Snapshot();
        Assert.Equal(10, s.Requests);
        Assert.Equal(1900, s.WireBytes);
        Assert.Equal(0, s.Throttled);
    }

    [Fact]
    public void Statistics_from_another_thread_reach_the_captured_handle()
    {
        var acc = new ConnectorStatsAccumulator();
        var bridge = new KafkaStatsBridge(acc);
        var t = new Thread(() => bridge.OnStatistics(First));
        t.Start();
        t.Join();
        Assert.Equal(4, acc.Snapshot().Requests);
    }

    [Fact]
    public void Malformed_statistics_are_ignored()
    {
        var acc = new ConnectorStatsAccumulator();
        new KafkaStatsBridge(acc).OnStatistics("{not json");
        Assert.True(acc.Snapshot().IsEmpty);
    }

    [Fact]
    public void A_counter_that_goes_backwards_adds_nothing()
    {
        var acc = new ConnectorStatsAccumulator();
        var bridge = new KafkaStatsBridge(acc);
        bridge.OnStatistics(Second);
        bridge.OnStatistics(First);
        Assert.Equal(10, acc.Snapshot().Requests);
    }

    [Fact]
    public void Messages_are_reported_as_an_extra()
    {
        var acc = new ConnectorStatsAccumulator();
        new KafkaStatsBridge(acc).CountMessages(7);
        Assert.Contains(new KeyValuePair<string, double>("kafka.messages", 7), acc.Snapshot().Extras);
    }

    [Fact]
    public void A_client_that_closes_before_any_tick_reports_one_request_once()
    {
        var acc = new ConnectorStatsAccumulator();
        var bridge = new KafkaStatsBridge(acc);
        bridge.Complete();
        bridge.Complete();
        Assert.Equal(1, acc.Snapshot().Requests);
    }

    [Fact]
    public void A_client_that_received_a_tick_adds_no_floor_at_close()
    {
        var acc = new ConnectorStatsAccumulator();
        var bridge = new KafkaStatsBridge(acc);
        bridge.OnStatistics(First);
        bridge.Complete();
        Assert.Equal(4, acc.Snapshot().Requests);
    }
}
