using System.Globalization;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace Pz.Connector.Kafka;

/// <summary>One row of an Arrow batch as a UTF-8 JSON object over a fixed projection of columns,
/// property order = schema order of the projection. Hand-written over the v0 type matrix: no
/// reflection, and the number/string choices are a documented contract (Decimal128 as a string to
/// keep precision; NaN/Infinity as null because JSON has no spelling for them).</summary>
internal sealed class RowJsonWriter(Schema schema, IReadOnlyList<int> columns)
{
    // The default encoder escapes ASCII punctuation (e.g. '"' as ") for safe HTML embedding,
    // which this record value is never rendered into; relaxed escaping keeps the JSON minimal
    // (\" for a quote) and is still spec-valid since only '"', '\', and control characters are escaped.
    private static readonly JsonWriterOptions Options = new() { Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping };

    private readonly string[] _names = columns.Select(i => schema.FieldsList[i].Name).ToArray();
    private readonly MemoryStream _buffer = new();

    public byte[] Write(RecordBatch batch, int row)
    {
        _buffer.SetLength(0);
        using (var writer = new Utf8JsonWriter(_buffer, Options))
        {
            writer.WriteStartObject();
            for (var c = 0; c < columns.Count; c++)
            {
                var column = batch.Column(columns[c]);
                writer.WritePropertyName(_names[c]);
                if (column.IsNull(row))
                {
                    writer.WriteNullValue();
                    continue;
                }

                switch (column)
                {
                    case Int32Array a: writer.WriteNumberValue(a.GetValue(row)!.Value); break;
                    case Int64Array a: writer.WriteNumberValue(a.GetValue(row)!.Value); break;
                    case DoubleArray a:
                        var d = a.GetValue(row)!.Value;
                        if (double.IsFinite(d)) writer.WriteNumberValue(d); else writer.WriteNullValue();
                        break;
                    case BooleanArray a: writer.WriteBooleanValue(a.GetValue(row)!.Value); break;
                    default: writer.WriteStringValue(ArrowScalars.Format(column, row)); break;
                }
            }

            writer.WriteEndObject();
        }

        return _buffer.ToArray();
    }
}

/// <summary>Scalar text formatting shared by JSON string values, record keys, and header values.</summary>
internal static class ArrowScalars
{
    public static string? Format(IArrowArray column, int row)
    {
        if (column.IsNull(row))
        {
            return null;
        }

        return column switch
        {
            StringArray a => a.GetString(row),
            Int32Array a => a.GetValue(row)!.Value.ToString(CultureInfo.InvariantCulture),
            Int64Array a => a.GetValue(row)!.Value.ToString(CultureInfo.InvariantCulture),
            DoubleArray a => a.GetValue(row)!.Value.ToString("R", CultureInfo.InvariantCulture),
            // Through SqlDecimal, never GetValue: System.Decimal holds 28-29 significant digits, and
            // for a wider Decimal128 GetValue returns the value with the excess digits dropped rather
            // than throwing, so a 38-digit amount would land in the topic silently truncated.
            // SqlDecimal.ToString is culture-independent and renders every stored digit at the
            // column's scale, which for values that fit is byte-identical to the decimal rendering.
            Decimal128Array a => a.GetSqlDecimal(row)!.Value.ToString(),
            BooleanArray a => a.GetValue(row)!.Value ? "true" : "false",
            Date32Array a => a.GetDateOnly(row)!.Value.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
            TimestampArray a => a.GetTimestamp(row)!.Value.UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss.ffffff'Z'", CultureInfo.InvariantCulture),
            _ => throw new NotSupportedException($"column type {column.Data.DataType.TypeId} is outside pz's type matrix"),
        };
    }

    public static byte[]? Bytes(IArrowArray column, int row) =>
        Format(column, row) is { } text ? Encoding.UTF8.GetBytes(text) : null;
}
