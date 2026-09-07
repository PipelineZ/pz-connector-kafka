using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Kafka;

/// <summary>Per-output write options. Column names are checked against the real schema at
/// BeginWriteAsync (<see cref="ValidateAgainst"/>); Parse only knows the option shapes. When no
/// <c>value:</c> column is named, the record value is every column not named by <c>key:</c> or
/// <c>headers:</c>, serialized as one JSON object.</summary>
internal sealed record KafkaOutputConfig(
    string Topic, string? KeyColumn, string? ValueColumn, IReadOnlyList<string> HeaderColumns, string Compression)
{
    private static readonly string[] KnownKeys = ["topic", "key", "value", "headers", "compression"];
    private static readonly string[] Compressions = ["none", "gzip", "snappy", "lz4", "zstd"];
    private static readonly ArrowTypeId[] KeyTypes = [ArrowTypeId.String, ArrowTypeId.Int32, ArrowTypeId.Int64];
    private static readonly ArrowTypeId[] HeaderTypes =
        [ArrowTypeId.String, ArrowTypeId.Int32, ArrowTypeId.Int64, ArrowTypeId.Boolean, ArrowTypeId.Date32, ArrowTypeId.Timestamp];

    /// <summary>Exactly what <c>RowJsonWriter</c> can spell -- pz's v0 type matrix. A column outside
    /// it must be refused here, while the errors still aggregate and before a producer exists;
    /// reaching the writer with one throws mid-batch, after rows have already been produced.</summary>
    private static readonly ArrowTypeId[] JsonTypes =
    [
        ArrowTypeId.String, ArrowTypeId.Int32, ArrowTypeId.Int64, ArrowTypeId.Double,
        ArrowTypeId.Decimal128, ArrowTypeId.Boolean, ArrowTypeId.Date32, ArrowTypeId.Timestamp,
    ];

    public static KafkaOutputConfig? Parse(OutputSpec spec, List<string> errors)
    {
        var start = errors.Count;
        var prefix = $"output '{spec.Output}'";
        foreach (var unknownKey in spec.Options.Keys.Where(k => !KnownKeys.Contains(k, StringComparer.Ordinal)))
        {
            errors.Add($"{prefix}: unknown write option '{unknownKey}'; known: topic, key, value, headers, compression");
        }

        var topic = spec.Output;
        if (spec.Options.TryGetValue("topic", out var topicRaw))
        {
            topic = topicRaw?.ToString() ?? "";
            if (topic.Length == 0)
            {
                errors.Add($"{prefix}: 'topic' must be a non-empty string");
            }
        }

        var key = Name(spec, "key", prefix, errors);
        var value = Name(spec, "value", prefix, errors);

        var headers = new List<string>();
        if (spec.Options.TryGetValue("headers", out var headersRaw) && headersRaw is not null)
        {
            if (headersRaw is IEnumerable<object?> list && headersRaw is not string)
            {
                foreach (var item in list)
                {
                    if (item?.ToString() is { Length: > 0 } name)
                    {
                        headers.Add(name);
                    }
                    else
                    {
                        errors.Add($"{prefix}: 'headers' entries must be non-empty column names");
                    }
                }
            }
            else
            {
                errors.Add($"{prefix}: 'headers' must be a list of column names");
            }
        }

        var compression = "none";
        if (spec.Options.TryGetValue("compression", out var compressionRaw) && compressionRaw is not null)
        {
            compression = compressionRaw.ToString()!;
            if (!Compressions.Contains(compression, StringComparer.Ordinal))
            {
                errors.Add($"{prefix}: 'compression' must be one of {string.Join(", ", Compressions)}; got '{compression}'");
            }
        }

        return errors.Count == start ? new KafkaOutputConfig(topic, key, value, headers.ToArray(), compression) : null;
    }

    public void ValidateAgainst(string output, Schema schema, List<string> errors)
    {
        var prefix = $"output '{output}'";
        Check(prefix, schema, "key", KeyColumn, KeyTypes, errors);
        Check(prefix, schema, "value", ValueColumn, [ArrowTypeId.String], errors);
        foreach (var header in HeaderColumns)
        {
            Check(prefix, schema, "headers", header, HeaderTypes, errors);
        }

        // Every column the record value is built from, which is empty when 'value:' names one.
        foreach (var index in JsonColumnIndexes(schema))
        {
            var field = schema.FieldsList[index];
            if (!JsonTypes.Contains(field.DataType.TypeId))
            {
                errors.Add($"{prefix}: column '{field.Name}' is {field.DataType.TypeId}, which the record value's JSON "
                    + $"cannot carry; allowed: {string.Join(", ", JsonTypes)}. Name a 'value' column, or drop it from the pipeline's projection");
            }
        }
    }

    public IReadOnlyList<int> JsonColumnIndexes(Schema schema)
    {
        if (ValueColumn is not null)
        {
            return [];
        }

        var excluded = new HashSet<string>(HeaderColumns, StringComparer.Ordinal);
        if (KeyColumn is not null)
        {
            excluded.Add(KeyColumn);
        }

        var fields = schema.FieldsList;
        return Enumerable.Range(0, fields.Count).Where(i => !excluded.Contains(fields[i].Name)).ToArray();
    }

    // HeaderColumns is an IReadOnlyList<string>: compiler-generated record equality compares it by
    // reference (List<T>/arrays don't override Equals), so two configs parsed from separately built
    // header lists would never compare equal. Give the record real value equality instead.
    public bool Equals(KafkaOutputConfig? other) =>
        other is not null
        && Topic == other.Topic
        && KeyColumn == other.KeyColumn
        && ValueColumn == other.ValueColumn
        && Compression == other.Compression
        && HeaderColumns.SequenceEqual(other.HeaderColumns, StringComparer.Ordinal);

    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(Topic);
        hash.Add(KeyColumn);
        hash.Add(ValueColumn);
        hash.Add(Compression);
        foreach (var header in HeaderColumns)
        {
            hash.Add(header);
        }

        return hash.ToHashCode();
    }

    private static string? Name(OutputSpec spec, string option, string prefix, List<string> errors)
    {
        if (!spec.Options.TryGetValue(option, out var raw) || raw is null)
        {
            return null;
        }

        var name = raw.ToString();
        if (string.IsNullOrEmpty(name))
        {
            errors.Add($"{prefix}: '{option}' must be a column name");
            return null;
        }

        return name;
    }

    private static void Check(string prefix, Schema schema, string option, string? column, ArrowTypeId[] allowed, List<string> errors)
    {
        if (column is null)
        {
            return;
        }

        var field = schema.FieldsList.FirstOrDefault(f => f.Name == column);
        if (field is null)
        {
            errors.Add($"{prefix}: '{option}' names column '{column}', which the pipeline does not produce");
        }
        else if (!allowed.Contains(field.DataType.TypeId))
        {
            errors.Add($"{prefix}: '{option}' column '{column}' is {field.DataType.TypeId}; allowed: {string.Join(", ", allowed)}");
        }
    }
}
