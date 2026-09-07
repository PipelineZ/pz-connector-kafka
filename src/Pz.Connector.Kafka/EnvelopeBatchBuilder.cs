using System.Text;
using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Abstractions.Batches;

namespace Pz.Connector.Kafka;

/// <summary>Pivots consumed records into the fixed envelope through the ABI's
/// <see cref="ArrowBatchBuilder"/>, so batch buffers come from the pooled native allocator the
/// engine expects. Key and value are text: UTF-8 decoded strictly (a byte sequence that is not
/// UTF-8 is a refusal naming the record, never a replacement character -- silently altered data is
/// worse than a stopped feed), or base64 when the dataset says so. Headers are metadata and never
/// stop a read: a non-UTF-8 header value lands base64 with a <c>base64:</c> prefix.</summary>
internal sealed class EnvelopeBatchBuilder
{
    public static readonly Schema Schema = new(
    [
        new Field("topic", StringType.Default, nullable: false),
        new Field("partition", Int32Type.Default, nullable: false),
        new Field("offset", Int64Type.Default, nullable: false),
        new Field("timestamp", new TimestampType(TimeUnit.Microsecond, "UTC"), nullable: false),
        new Field("key", StringType.Default, nullable: true),
        new Field("value", StringType.Default, nullable: true),
        new Field("headers", StringType.Default, nullable: false),
    ], null);

    private static readonly UTF8Encoding StrictUtf8 = new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

    private readonly ArrowBatchBuilder _inner;
    private readonly PayloadEncoding _encoding;
    private readonly KafkaRedactor _redactor;
    private readonly object?[] _row = new object?[7];

    public EnvelopeBatchBuilder(PayloadEncoding encoding, BatchOptions options, KafkaRedactor redactor)
    {
        _inner = new ArrowBatchBuilder(Schema, options.TargetBatchBytes, maxRowsPerBatch: options.MaxRowsPerBatch);
        _encoding = encoding;
        _redactor = redactor;
    }

    public int PendingRows => _inner.PendingRows;

    public void Append(string topic, int partition, long offset, DateTimeOffset timestamp, byte[]? key, byte[]? value,
        IReadOnlyList<KeyValuePair<string, byte[]?>> headers)
    {
        _row[0] = topic;
        _row[1] = partition;
        _row[2] = offset;
        _row[3] = timestamp.ToUniversalTime();
        _row[4] = Decode(key, topic, partition, offset, "key");
        _row[5] = Decode(value, topic, partition, offset, "value");
        _row[6] = HeadersToJson(headers);
        _inner.AppendRow(_row);
    }

    public bool TryTakeBatch(out RecordBatch? batch) => _inner.TryTakeBatch(out batch);

    public RecordBatch? Flush() => _inner.Flush();

    public static string HeadersToJson(IReadOnlyList<KeyValuePair<string, byte[]?>> headers)
    {
        if (headers.Count == 0)
        {
            return "{}";
        }

        // Insertion-ordered grouping: the first occurrence fixes a name's position, repeats append.
        var order = new List<string>();
        var values = new Dictionary<string, List<string?>>(StringComparer.Ordinal);
        foreach (var (name, raw) in headers)
        {
            if (!values.TryGetValue(name, out var list))
            {
                list = [];
                values[name] = list;
                order.Add(name);
            }

            list.Add(raw is null ? null : TryUtf8(raw) ?? "base64:" + Convert.ToBase64String(raw));
        }

        var buffer = new MemoryStream();
        using (var writer = new Utf8JsonWriter(buffer))
        {
            writer.WriteStartObject();
            foreach (var name in order)
            {
                var list = values[name];
                writer.WritePropertyName(name);
                if (list.Count == 1)
                {
                    WriteValue(writer, list[0]);
                }
                else
                {
                    writer.WriteStartArray();
                    foreach (var item in list)
                    {
                        WriteValue(writer, item);
                    }

                    writer.WriteEndArray();
                }
            }

            writer.WriteEndObject();
        }

        return Encoding.UTF8.GetString(buffer.ToArray());

        static void WriteValue(Utf8JsonWriter writer, string? value)
        {
            if (value is null)
            {
                writer.WriteNullValue();
            }
            else
            {
                writer.WriteStringValue(value);
            }
        }
    }

    private string? Decode(byte[]? bytes, string topic, int partition, long offset, string which)
    {
        if (bytes is null)
        {
            return null;
        }

        if (_encoding == PayloadEncoding.Base64)
        {
            return Convert.ToBase64String(bytes);
        }

        return TryUtf8(bytes) ?? throw KafkaErrors.Fatal(
            $"topic '{topic}' partition {partition} offset {offset}: the record {which} is not valid UTF-8; " +
            "set `encoding: base64` on the dataset to land raw bytes as base64 text", _redactor);
    }

    private static string? TryUtf8(byte[] bytes)
    {
        try
        {
            return StrictUtf8.GetString(bytes);
        }
        catch (DecoderFallbackException)
        {
            return null;
        }
    }
}
