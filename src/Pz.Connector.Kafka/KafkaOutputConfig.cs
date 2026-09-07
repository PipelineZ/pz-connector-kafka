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

        // Array.Empty<string>() is a cached singleton, so an empty-headers config compares equal
        // (record equality on IReadOnlyList<string> falls back to reference equality) to a literal
        // `[]` default, which the compiler also lowers to that same singleton.
        var headerColumns = headers.Count == 0 ? System.Array.Empty<string>() : headers.ToArray();
        return errors.Count == start ? new KafkaOutputConfig(topic, key, value, headerColumns, compression) : null;
    }

    public void ValidateAgainst(Schema schema, List<string> errors)
    {
        Check(schema, "key", KeyColumn, KeyTypes, errors);
        Check(schema, "value", ValueColumn, [ArrowTypeId.String], errors);
        foreach (var header in HeaderColumns)
        {
            Check(schema, "headers", header, HeaderTypes, errors);
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

    private static void Check(Schema schema, string option, string? column, ArrowTypeId[] allowed, List<string> errors)
    {
        if (column is null)
        {
            return;
        }

        var field = schema.FieldsList.FirstOrDefault(f => f.Name == column);
        if (field is null)
        {
            errors.Add($"'{option}' names column '{column}', which the pipeline does not produce");
        }
        else if (!allowed.Contains(field.DataType.TypeId))
        {
            errors.Add($"'{option}' column '{column}' is {field.DataType.TypeId}; allowed: {string.Join(", ", allowed)}");
        }
    }
}
