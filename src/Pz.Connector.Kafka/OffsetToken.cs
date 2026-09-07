using System.Text;
using System.Text.Json;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Kafka;

/// <summary>The dataset's sync-state token: the next offset to read per partition of one topic.
/// Written with stable key order so the engine's stored state is byte-stable across runs that
/// changed nothing. Parsing never places the token text in an error: it is engine-opaque state
/// and must not surface in run artifacts.</summary>
internal sealed record OffsetToken(string Topic, IReadOnlyDictionary<int, long> NextOffsets)
{
    public const int Version = 1;

    public string Serialize()
    {
        var buffer = new MemoryStream();
        using (var writer = new Utf8JsonWriter(buffer))
        {
            writer.WriteStartObject();
            writer.WriteNumber("v", Version);
            writer.WriteString("topic", Topic);
            writer.WriteStartObject("partitions");
            foreach (var partition in NextOffsets.Keys.Order())
            {
                writer.WriteNumber(partition.ToString(System.Globalization.CultureInfo.InvariantCulture), NextOffsets[partition]);
            }

            writer.WriteEndObject();
            writer.WriteEndObject();
        }

        return Encoding.UTF8.GetString(buffer.ToArray());
    }

    public static OffsetToken Parse(string json, string expectedTopic, KafkaRedactor redactor)
    {
        static PzConnectorException Malformed(KafkaRedactor redactor, string why) => KafkaErrors.Fatal(
            $"stored sync state is not a kafka offset token ({why}); run with --full-refresh or clear the dataset's state with `pz state`", redactor);

        JsonDocument document;
        try
        {
            document = JsonDocument.Parse(json);
        }
        catch (JsonException)
        {
            throw Malformed(redactor, "not JSON");
        }

        using (document)
        {
            var root = document.RootElement;
            if (root.ValueKind != JsonValueKind.Object
                || !root.TryGetProperty("v", out var v) || !v.TryGetInt32(out var version))
            {
                throw Malformed(redactor, "missing version");
            }

            if (version != Version)
            {
                throw Malformed(redactor, $"version {version}, expected {Version}");
            }

            if (!root.TryGetProperty("topic", out var topicElement) || topicElement.ValueKind != JsonValueKind.String
                || !root.TryGetProperty("partitions", out var partitions) || partitions.ValueKind != JsonValueKind.Object)
            {
                throw Malformed(redactor, "missing topic or partitions");
            }

            var topic = topicElement.GetString()!;
            if (!string.Equals(topic, expectedTopic, StringComparison.Ordinal))
            {
                throw KafkaErrors.Fatal(
                    $"stored sync state belongs to topic '{topic}' but the dataset now reads topic '{expectedTopic}'; " +
                    "run with --full-refresh to start over, or point the dataset back at the original topic", redactor);
            }

            var offsets = new Dictionary<int, long>();
            foreach (var property in partitions.EnumerateObject())
            {
                if (!int.TryParse(property.Name, System.Globalization.NumberStyles.None,
                        System.Globalization.CultureInfo.InvariantCulture, out var partition)
                    || !property.Value.TryGetInt64(out var offset) || offset < 0)
                {
                    throw Malformed(redactor, "bad partition entry");
                }

                offsets[partition] = offset;
            }

            return new OffsetToken(topic, offsets);
        }
    }
}
