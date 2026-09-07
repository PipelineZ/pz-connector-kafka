using Confluent.Kafka;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Kafka;

/// <summary>Turns Confluent/librdkafka failures into the engine's exception, classified for retry.
/// Transient = the broker or the network may recover on its own; everything about credentials,
/// authorization, topic existence, offsets, and sizes is not. Unmapped codes are non-transient:
/// an unknown failure retried is a failure hidden. Messages always pass the redactor -- librdkafka
/// echoes configuration into some of its reasons.</summary>
internal static class KafkaErrors
{
    public static bool IsTransient(ErrorCode code) => code is
        ErrorCode.Local_Transport or ErrorCode.Local_TimedOut or ErrorCode.Local_AllBrokersDown
        or ErrorCode.BrokerNotAvailable or ErrorCode.LeaderNotAvailable or ErrorCode.NotLeaderForPartition
        or ErrorCode.RequestTimedOut or ErrorCode.NetworkException or ErrorCode.NotEnoughReplicas
        or ErrorCode.NotEnoughReplicasAfterAppend or ErrorCode.Local_QueueFull
        or ErrorCode.GroupCoordinatorNotAvailable;

    public static PzConnectorException Wrap(Exception ex, KafkaRedactor redactor, string context)
    {
        if (ex is PzConnectorException already)
        {
            return already;
        }

        if (ex is KafkaException kafka)
        {
            var code = kafka.Error.Code;
            return new PzConnectorException(
                redactor.Redact($"kafka: {context}: {code} -- {kafka.Error.Reason}"),
                IsTransient(code), innerException: ex);
        }

        return new PzConnectorException(redactor.Redact($"kafka: {context}: {ex.Message}"), isTransient: false, innerException: ex);
    }

    public static PzConnectorException Fatal(string message, KafkaRedactor redactor) =>
        new(redactor.Redact($"kafka: {message}"), isTransient: false);

    public static PzConnectorException Transient(string message, KafkaRedactor redactor) =>
        new(redactor.Redact($"kafka: {message}"), isTransient: true);
}
