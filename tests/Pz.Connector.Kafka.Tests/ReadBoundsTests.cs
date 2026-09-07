using Pz.Connectors.Abstractions;

namespace Pz.Connector.Kafka.Tests;

public sealed class ReadBoundsTests
{
    private static readonly StartPosition Earliest = new(StartKind.Earliest, null);
    private static readonly StartPosition Latest = new(StartKind.Latest, null);
    private static readonly StartPosition AtTime = new(StartKind.Timestamp, DateTimeOffset.UnixEpoch);

    private static PartitionWatermarks Wm(int p, long low, long high) => new(p, low, high);

    [Fact]
    public void No_token_earliest_resumes_at_low()
    {
        var bounds = ReadBounds.Compute("t", null, Earliest, [Wm(0, 10, 50), Wm(1, 0, 0)], null, KafkaRedactor.None);

        Assert.Equal([new PartitionBound(0, 10, 50), new PartitionBound(1, 0, 0)], bounds);
        Assert.False(bounds[0].Done);
        Assert.True(bounds[1].Done);
    }

    [Fact]
    public void No_token_latest_resumes_at_high_so_everything_is_done()
    {
        var bounds = ReadBounds.Compute("t", null, Latest, [Wm(0, 10, 50)], null, KafkaRedactor.None);

        Assert.Equal([new PartitionBound(0, 50, 50)], bounds);
    }

    [Fact]
    public void No_token_timestamp_uses_the_resolved_offset_or_high_when_unresolved()
    {
        var bounds = ReadBounds.Compute("t", null, AtTime, [Wm(0, 10, 50), Wm(1, 0, 20)],
            new Dictionary<int, long> { [0] = 30 }, KafkaRedactor.None);

        Assert.Equal([new PartitionBound(0, 30, 50), new PartitionBound(1, 20, 20)], bounds);
    }

    [Fact]
    public void Token_resumes_each_partition_and_new_partitions_use_start()
    {
        var token = new OffsetToken("t", new Dictionary<int, long> { [0] = 20 });

        var bounds = ReadBounds.Compute("t", token, Latest, [Wm(0, 10, 50), Wm(1, 0, 7)], null, KafkaRedactor.None);

        Assert.Equal([new PartitionBound(0, 20, 50), new PartitionBound(1, 7, 7)], bounds);
    }

    [Fact]
    public void Token_at_high_is_done_and_token_at_low_is_fine()
    {
        var token = new OffsetToken("t", new Dictionary<int, long> { [0] = 50, [1] = 10 });

        var bounds = ReadBounds.Compute("t", token, Earliest, [Wm(0, 10, 50), Wm(1, 10, 50)], null, KafkaRedactor.None);

        Assert.True(bounds[0].Done);
        Assert.Equal(10, bounds[1].Resume);
    }

    [Fact]
    public void Token_below_low_is_retention_loss()
    {
        var token = new OffsetToken("t", new Dictionary<int, long> { [0] = 5 });

        var ex = Assert.Throws<PzConnectorException>(() =>
            ReadBounds.Compute("t", token, Earliest, [Wm(0, 10, 50)], null, KafkaRedactor.None));

        Assert.False(ex.IsTransient);
        Assert.Contains("partition 0", ex.Message);
        Assert.Contains("stored offset 5", ex.Message);
        Assert.Contains("earliest available 10", ex.Message);
        Assert.Contains("--full-refresh", ex.Message);
    }

    [Fact]
    public void Token_above_high_is_an_offset_reset()
    {
        var token = new OffsetToken("t", new Dictionary<int, long> { [0] = 99 });

        var ex = Assert.Throws<PzConnectorException>(() =>
            ReadBounds.Compute("t", token, Earliest, [Wm(0, 10, 50)], null, KafkaRedactor.None));

        Assert.False(ex.IsTransient);
        Assert.Contains("partition 0", ex.Message);
        Assert.Contains("99", ex.Message);
        Assert.Contains("50", ex.Message);
    }

    [Fact]
    public void Every_error_is_reported_together()
    {
        var token = new OffsetToken("t", new Dictionary<int, long> { [0] = 5, [1] = 99 });

        var ex = Assert.Throws<PzConnectorException>(() =>
            ReadBounds.Compute("t", token, Earliest, [Wm(0, 10, 50), Wm(1, 0, 50)], null, KafkaRedactor.None));

        Assert.Contains("partition 0", ex.Message);
        Assert.Contains("partition 1", ex.Message);
    }

    [Fact]
    public void Output_is_ordered_by_partition()
    {
        var bounds = ReadBounds.Compute("t", null, Earliest, [Wm(2, 0, 1), Wm(0, 0, 1), Wm(1, 0, 1)], null, KafkaRedactor.None);

        Assert.Equal([0, 1, 2], bounds.Select(b => b.Partition));
    }
}
