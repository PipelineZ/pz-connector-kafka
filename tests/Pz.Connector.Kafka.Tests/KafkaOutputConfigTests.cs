using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Kafka.Tests;

public sealed class KafkaOutputConfigTests
{
    private static readonly Schema Schema = new(
    [
        new Field("id", Int64Type.Default, true),
        new Field("name", StringType.Default, true),
        new Field("src", StringType.Default, true),
        new Field("amount", DoubleType.Default, true),
        new Field("payload", StringType.Default, true),
    ], null);

    private static OutputSpec Spec(Dictionary<string, object?> options, string output = "order-events") =>
        new("events", output, "append", "fail_on_change", options);

    [Fact]
    public void Defaults_to_entity_topic_whole_row_no_key_no_headers_no_compression()
    {
        var errors = new List<string>();
        var config = KafkaOutputConfig.Parse(Spec([]), errors);

        Assert.Empty(errors);
        Assert.Equal(new KafkaOutputConfig("order-events", null, null, [], "none"), config);
        Assert.Equal([0, 1, 2, 3, 4], config!.JsonColumnIndexes(Schema));
    }

    [Fact]
    public void Key_and_headers_are_excluded_from_the_json_value()
    {
        var errors = new List<string>();
        var config = KafkaOutputConfig.Parse(Spec(new()
        {
            ["topic"] = "t", ["key"] = "id", ["headers"] = new List<object?> { "src" }, ["compression"] = "zstd",
        }), errors);

        Assert.Empty(errors);
        config!.ValidateAgainst("order-events", Schema, errors);
        Assert.Empty(errors);
        Assert.Equal([1, 3, 4], config.JsonColumnIndexes(Schema));
        Assert.Equal("zstd", config.Compression);
    }

    [Fact]
    public void Configs_parsed_from_separately_built_header_lists_compare_equal()
    {
        var errors = new List<string>();
        var a = KafkaOutputConfig.Parse(Spec(new() { ["headers"] = new List<object?> { "src", "id" } }), errors);
        var b = KafkaOutputConfig.Parse(Spec(new() { ["headers"] = new List<object?> { "src", "id" } }), errors);

        Assert.Equal(a, b);
    }

    [Fact]
    public void Explicit_value_column_excludes_nothing_else()
    {
        var errors = new List<string>();
        var config = KafkaOutputConfig.Parse(Spec(new() { ["value"] = "payload", ["key"] = "id" }), errors);

        config!.ValidateAgainst("order-events", Schema, errors);
        Assert.Empty(errors);
        Assert.Empty(config.JsonColumnIndexes(Schema));
    }

    [Fact]
    public void Parse_reports_bad_options()
    {
        var errors = new List<string>();
        var config = KafkaOutputConfig.Parse(Spec(new()
        {
            ["compression"] = "brotli", ["headers"] = "src", ["nope"] = 1L, ["topic"] = "",
        }), errors);

        Assert.Null(config);
        Assert.Equal(4, errors.Count);
        Assert.Contains(errors, e => e.Contains("'compression'"));
        Assert.Contains(errors, e => e.Contains("'headers'") && e.Contains("list"));
        Assert.Contains(errors, e => e.Contains("'nope'"));
        Assert.Contains(errors, e => e.Contains("'topic'"));
    }

    [Fact]
    public void ValidateAgainst_refuses_a_column_the_record_json_cannot_carry()
    {
        var schema = new Schema([new Field("id", Int64Type.Default, true), new Field("ratio", FloatType.Default, true)], null);
        var errors = new List<string>();
        var config = KafkaOutputConfig.Parse(Spec([]), errors)!;

        config.ValidateAgainst("order-events", schema, errors);

        // Named by column and type, and aggregated like every other schema error -- not thrown
        // out of the row writer once the producer is already running.
        var error = Assert.Single(errors);
        Assert.Contains("output 'order-events':", error);
        Assert.Contains("'ratio'", error);
        Assert.Contains("Float", error);
    }

    [Fact]
    public void ValidateAgainst_reports_missing_and_mistyped_columns()
    {
        var errors = new List<string>();
        var config = KafkaOutputConfig.Parse(Spec(new()
        {
            ["key"] = "amount", ["value"] = "id", ["headers"] = new List<object?> { "missing", "amount" },
        }), errors)!;

        config.ValidateAgainst("order-events", Schema, errors);

        Assert.Equal(4, errors.Count);
        Assert.All(errors, e => Assert.Contains("output 'order-events':", e));
        Assert.Contains(errors, e => e.Contains("'key'") && e.Contains("amount") && e.Contains("Double"));
        Assert.Contains(errors, e => e.Contains("'value'") && e.Contains("id") && e.Contains("String"));
        Assert.Contains(errors, e => e.Contains("'headers'") && e.Contains("missing"));
        Assert.Contains(errors, e => e.Contains("'headers'") && e.Contains("amount"));
    }
}
