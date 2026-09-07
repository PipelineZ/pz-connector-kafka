using Confluent.Kafka;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Kafka.Tests;

public sealed class KafkaErrorsTests
{
    [Theory]
    [InlineData(ErrorCode.Local_Transport, true)]
    [InlineData(ErrorCode.Local_TimedOut, true)]
    [InlineData(ErrorCode.Local_MsgTimedOut, true)]
    [InlineData(ErrorCode.Local_AllBrokersDown, true)]
    [InlineData(ErrorCode.BrokerNotAvailable, true)]
    [InlineData(ErrorCode.LeaderNotAvailable, true)]
    [InlineData(ErrorCode.NotLeaderForPartition, true)]
    [InlineData(ErrorCode.RequestTimedOut, true)]
    [InlineData(ErrorCode.NetworkException, true)]
    [InlineData(ErrorCode.NotEnoughReplicas, true)]
    [InlineData(ErrorCode.NotEnoughReplicasAfterAppend, true)]
    [InlineData(ErrorCode.Local_QueueFull, true)]
    [InlineData(ErrorCode.GroupCoordinatorNotAvailable, true)]
    [InlineData(ErrorCode.SaslAuthenticationFailed, false)]
    [InlineData(ErrorCode.TopicAuthorizationFailed, false)]
    [InlineData(ErrorCode.GroupAuthorizationFailed, false)]
    [InlineData(ErrorCode.ClusterAuthorizationFailed, false)]
    [InlineData(ErrorCode.UnknownTopicOrPart, false)]
    [InlineData(ErrorCode.OffsetOutOfRange, false)]
    [InlineData(ErrorCode.Local_UnknownTopic, false)]
    [InlineData(ErrorCode.Local_UnknownPartition, false)]
    [InlineData(ErrorCode.InvalidMsgSize, false)]
    [InlineData(ErrorCode.MsgSizeTooLarge, false)]
    [InlineData(ErrorCode.Unknown, false)]
    public void Classifies_error_codes(ErrorCode code, bool transient)
    {
        Assert.Equal(transient, KafkaErrors.IsTransient(code));
    }

    [Fact]
    public void Wrap_redacts_and_names_the_context()
    {
        var redactor = new KafkaRedactor(["hunter2"]);
        var inner = new KafkaException(new Error(ErrorCode.SaslAuthenticationFailed, "auth failed for hunter2"));

        var ex = KafkaErrors.Wrap(inner, redactor, "topic 'orders'");

        Assert.False(ex.IsTransient);
        Assert.DoesNotContain("hunter2", ex.Message);
        Assert.Contains("topic 'orders'", ex.Message);
        Assert.Contains("SaslAuthenticationFailed", ex.Message);
        Assert.Same(inner, ex.InnerException);
    }

    [Fact]
    public void Wrap_of_a_non_kafka_exception_is_non_transient()
    {
        var ex = KafkaErrors.Wrap(new InvalidOperationException("boom"), KafkaRedactor.None, "ctx");

        Assert.False(ex.IsTransient);
        Assert.Contains("boom", ex.Message);
    }

    [Fact]
    public void Wrap_passes_through_an_existing_connector_exception()
    {
        var original = new PzConnectorException("already", isTransient: true);

        Assert.Same(original, KafkaErrors.Wrap(original, KafkaRedactor.None, "ctx"));
    }
}
