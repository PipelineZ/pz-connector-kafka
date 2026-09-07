namespace Pz.Connector.Kafka.Tests;

public sealed class KafkaRedactorTests
{
    [Fact]
    public void Replaces_every_occurrence_of_every_secret()
    {
        var redactor = new KafkaRedactor(["s3cret", "other"]);

        Assert.Equal("a *** b *** c ***", redactor.Redact("a s3cret b other c s3cret"));
    }

    [Fact]
    public void Rewrites_password_fragments_librdkafka_echoes_even_when_the_value_is_unknown()
    {
        var redactor = new KafkaRedactor([]);

        Assert.Equal("conf: sasl.password=*** ssl.key.password=***, bootstrap.servers=b:9092",
            redactor.Redact("conf: sasl.password=abc ssl.key.password=\"d e\", bootstrap.servers=b:9092"));
    }

    [Fact]
    public void Empty_and_short_secrets_are_ignored()
    {
        var redactor = new KafkaRedactor(["", "ab"]);

        Assert.Equal("abc", redactor.Redact("abc"));
    }

    [Fact]
    public void None_is_identity()
    {
        Assert.Equal("x", KafkaRedactor.None.Redact("x"));
    }
}
