using System.Text;
using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Kafka.Tests;

public sealed class EnvelopeBatchBuilderTests
{
    private static readonly DateTimeOffset T0 = new(2026, 9, 7, 10, 0, 0, TimeSpan.Zero);

    private static EnvelopeBatchBuilder Builder(PayloadEncoding encoding = PayloadEncoding.Utf8, int targetBytes = 32 * 1024 * 1024) =>
        new(encoding, new BatchOptions(targetBytes, 122_880), KafkaRedactor.None);

    [Fact]
    public void Schema_is_the_fixed_envelope()
    {
        var fields = EnvelopeBatchBuilder.Schema.FieldsList;

        Assert.Equal(["topic", "partition", "offset", "timestamp", "key", "value", "headers"], fields.Select(f => f.Name));
        Assert.Equal(ArrowTypeId.String, fields[0].DataType.TypeId);
        Assert.Equal(ArrowTypeId.Int32, fields[1].DataType.TypeId);
        Assert.Equal(ArrowTypeId.Int64, fields[2].DataType.TypeId);
        var ts = Assert.IsType<TimestampType>(fields[3].DataType);
        Assert.Equal(TimeUnit.Microsecond, ts.Unit);
        Assert.Equal("UTC", ts.Timezone);
        Assert.True(fields[4].IsNullable);
        Assert.True(fields[5].IsNullable);
        Assert.False(fields[6].IsNullable);
    }

    [Fact]
    public void Lands_a_utf8_record()
    {
        var builder = Builder();
        builder.Append("orders", 1, 42, T0, Encoding.UTF8.GetBytes("k1"), Encoding.UTF8.GetBytes("{\"a\":1}"),
            [new("h", Encoding.UTF8.GetBytes("v"))]);

        using var batch = builder.Flush()!;

        Assert.Equal(1, batch.Length);
        Assert.Equal("orders", ((StringArray)batch.Column(0)).GetString(0));
        Assert.Equal(1, ((Int32Array)batch.Column(1)).GetValue(0));
        Assert.Equal(42L, ((Int64Array)batch.Column(2)).GetValue(0));
        Assert.Equal(T0, ((TimestampArray)batch.Column(3)).GetTimestamp(0));
        Assert.Equal("k1", ((StringArray)batch.Column(4)).GetString(0));
        Assert.Equal("{\"a\":1}", ((StringArray)batch.Column(5)).GetString(0));
        Assert.Equal("{\"h\":\"v\"}", ((StringArray)batch.Column(6)).GetString(0));
    }

    [Fact]
    public void Null_key_and_value_land_as_null_and_no_headers_as_empty_object()
    {
        var builder = Builder();
        builder.Append("t", 0, 0, T0, null, null, []);

        using var batch = builder.Flush()!;

        Assert.True(batch.Column(4).IsNull(0));
        Assert.True(batch.Column(5).IsNull(0));
        Assert.Equal("{}", ((StringArray)batch.Column(6)).GetString(0));
    }

    [Fact]
    public void Invalid_utf8_fails_naming_the_record_and_the_fix()
    {
        var builder = Builder();

        var ex = Assert.Throws<PzConnectorException>(() =>
            builder.Append("orders", 3, 7, T0, null, [0xFF, 0xFE, 0x00], []));

        Assert.False(ex.IsTransient);
        Assert.Contains("topic 'orders'", ex.Message);
        Assert.Contains("partition 3", ex.Message);
        Assert.Contains("offset 7", ex.Message);
        Assert.Contains("encoding: base64", ex.Message);
    }

    [Fact]
    public void Base64_encodes_key_and_value_but_not_headers()
    {
        var builder = Builder(PayloadEncoding.Base64);
        builder.Append("t", 0, 0, T0, [1, 2, 3], [0xFF], [new("h", Encoding.UTF8.GetBytes("plain"))]);

        using var batch = builder.Flush()!;

        Assert.Equal(Convert.ToBase64String([1, 2, 3]), ((StringArray)batch.Column(4)).GetString(0));
        Assert.Equal(Convert.ToBase64String([0xFF]), ((StringArray)batch.Column(5)).GetString(0));
        Assert.Equal("{\"h\":\"plain\"}", ((StringArray)batch.Column(6)).GetString(0));
    }

    [Fact]
    public void Headers_collapse_repeats_into_arrays_and_base64_non_utf8_values()
    {
        var json = EnvelopeBatchBuilder.HeadersToJson(
        [
            new("a", Encoding.UTF8.GetBytes("1")),
            new("b", null),
            new("a", Encoding.UTF8.GetBytes("2")),
            new("c", [0xFF]),
        ]);

        Assert.Equal("{\"a\":[\"1\",\"2\"],\"b\":null,\"c\":\"base64:/w==\"}", json);
    }

    [Fact]
    public void Header_names_and_values_are_json_escaped()
    {
        const string Name = "a\"b\\c";
        const string Value = "v\"1\\2";

        var json = EnvelopeBatchBuilder.HeadersToJson([new(Name, Encoding.UTF8.GetBytes(Value))]);

        using var parsed = JsonDocument.Parse(json);
        Assert.Equal(Value, parsed.RootElement.GetProperty(Name).GetString());
    }

    [Fact]
    public void Batches_split_at_the_byte_target()
    {
        var builder = Builder(targetBytes: 512);
        var value = Encoding.UTF8.GetBytes(new string('x', 200));
        var taken = 0;
        for (var i = 0; i < 10; i++)
        {
            builder.Append("t", 0, i, T0, null, value, []);
            if (builder.TryTakeBatch(out var batch))
            {
                taken++;
                batch!.Dispose();
            }
        }

        var last = builder.Flush();
        last?.Dispose();

        Assert.True(taken >= 2, $"expected at least two batches, took {taken}");
    }
}
