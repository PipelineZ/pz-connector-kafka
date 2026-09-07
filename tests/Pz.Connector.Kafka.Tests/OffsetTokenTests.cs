using Pz.Connectors.Abstractions;

namespace Pz.Connector.Kafka.Tests;

public sealed class OffsetTokenTests
{
    [Fact]
    public void Serializes_with_stable_key_order()
    {
        var token = new OffsetToken("orders", new Dictionary<int, long> { [2] = 5, [0] = 1234, [1] = 88 });

        Assert.Equal("""{"v":1,"topic":"orders","partitions":{"0":1234,"1":88,"2":5}}""", token.Serialize());
    }

    [Fact]
    public void Round_trips()
    {
        var token = new OffsetToken("orders", new Dictionary<int, long> { [0] = 1, [1] = 2 });

        var parsed = OffsetToken.Parse(token.Serialize(), "orders", KafkaRedactor.None);

        Assert.Equal("orders", parsed.Topic);
        Assert.Equal(1, parsed.NextOffsets[0]);
        Assert.Equal(2, parsed.NextOffsets[1]);
    }

    [Fact]
    public void Refuses_topic_mismatch_non_transiently()
    {
        var json = new OffsetToken("orders", new Dictionary<int, long> { [0] = 1 }).Serialize();

        var ex = Assert.Throws<PzConnectorException>(() => OffsetToken.Parse(json, "returns", KafkaRedactor.None));

        Assert.False(ex.IsTransient);
        Assert.Contains("'orders'", ex.Message);
        Assert.Contains("'returns'", ex.Message);
        Assert.Contains("--full-refresh", ex.Message);
    }

    [Theory]
    [InlineData("""{"v":2,"topic":"orders","partitions":{}}""")]
    [InlineData("""{"topic":"orders","partitions":{}}""")]
    [InlineData("""{"v":1,"topic":"orders"}""")]
    [InlineData("""{"v":1,"topic":"orders","partitions":{"a":1}}""")]
    [InlineData("not json")]
    public void Refuses_malformed_tokens(string json)
    {
        var ex = Assert.Throws<PzConnectorException>(() => OffsetToken.Parse(json, "orders", KafkaRedactor.None));

        Assert.False(ex.IsTransient);
        Assert.DoesNotContain(json, ex.Message);
    }
}
