using Pz.Connectors.Abstractions;

namespace Pz.Connector.Kafka.Tests;

public sealed class KafkaConnectionConfigTests
{
    private static ConnectorConfig Config(Dictionary<string, object?> values) => new(values);

    [Fact]
    public void Minimal_config_maps_bootstrap_servers_and_plaintext_defaults()
    {
        var errors = new List<string>();
        var config = KafkaConnectionConfig.Parse(Config(new() { ["bootstrap_servers"] = "b1:9092,b2:9092" }), errors);

        Assert.Empty(errors);
        Assert.NotNull(config);
        Assert.Equal("b1:9092,b2:9092", config.ClientProperties["bootstrap.servers"]);
        Assert.Equal("plaintext", config.ClientProperties["security.protocol"]);
        Assert.Equal("pz", config.ClientProperties["client.id"]);
        Assert.Equal(60, config.IdleTimeoutSeconds);
        Assert.Null(config.GroupId);
        Assert.False(config.ClientProperties.ContainsKey("group.id"));
    }

    [Fact]
    public void Sasl_ssl_maps_every_typed_key()
    {
        var errors = new List<string>();
        var config = KafkaConnectionConfig.Parse(Config(new()
        {
            ["bootstrap_servers"] = "b:9092",
            ["security"] = "sasl_ssl",
            ["sasl"] = new Dictionary<string, object?>
            {
                ["mechanism"] = "SCRAM-SHA-512", ["username"] = "u", ["password"] = "p-secret",
            },
            ["ssl"] = new Dictionary<string, object?>
            {
                ["ca_location"] = "/abs/ca.pem", ["key_password"] = "k-secret",
            },
            ["client_id"] = "my-app",
            ["idle_timeout"] = 5L,
            ["group_id"] = "g",
        }), errors);

        Assert.Empty(errors);
        Assert.NotNull(config);
        Assert.Equal("sasl_ssl", config.ClientProperties["security.protocol"]);
        Assert.Equal("SCRAM-SHA-512", config.ClientProperties["sasl.mechanism"]);
        Assert.Equal("u", config.ClientProperties["sasl.username"]);
        Assert.Equal("p-secret", config.ClientProperties["sasl.password"]);
        Assert.Equal("/abs/ca.pem", config.ClientProperties["ssl.ca.location"]);
        Assert.Equal("k-secret", config.ClientProperties["ssl.key.password"]);
        Assert.Equal("my-app", config.ClientProperties["client.id"]);
        Assert.Equal(5, config.IdleTimeoutSeconds);
        Assert.Equal("g", config.GroupId);
        Assert.Equal("g", config.ClientProperties["group.id"]);
        Assert.Equal("sasl.password=***; ssl.key.password=***", config.Redactor.Redact("sasl.password=p-secret; ssl.key.password=k-secret"));
    }

    [Fact]
    public void Relative_ssl_paths_resolve_against_base_dir()
    {
        var errors = new List<string>();
        var config = KafkaConnectionConfig.Parse(Config(new()
        {
            ["bootstrap_servers"] = "b:9092",
            ["security"] = "ssl",
            ["ssl"] = new Dictionary<string, object?> { ["ca_location"] = "certs/ca.pem" },
            ["base_dir"] = "/proj",
        }), errors);

        Assert.Empty(errors);
        Assert.Equal(Path.Combine("/proj", "certs/ca.pem"), config!.ClientProperties["ssl.ca.location"]);
    }

    [Fact]
    public void An_already_rooted_ssl_path_ignores_base_dir()
    {
        var errors = new List<string>();
        var rooted = Path.Combine(Path.GetTempPath(), "ca.pem");
        var config = KafkaConnectionConfig.Parse(Config(new()
        {
            ["bootstrap_servers"] = "b:9092",
            ["security"] = "ssl",
            ["ssl"] = new Dictionary<string, object?> { ["ca_location"] = rooted },
            ["base_dir"] = "/proj",
        }), errors);

        Assert.Empty(errors);
        Assert.Equal(rooted, config!.ClientProperties["ssl.ca.location"]);
    }

    [Fact]
    public void Client_escape_hatch_passes_through_unknown_keys_and_refuses_typed_overlap()
    {
        var errors = new List<string>();
        var config = KafkaConnectionConfig.Parse(Config(new()
        {
            ["bootstrap_servers"] = "b:9092",
            ["client"] = new Dictionary<string, object?>
            {
                ["socket.timeout.ms"] = 30000L,
                ["security.protocol"] = "ssl",
                ["sasl.password"] = "x",
            },
        }), errors);

        Assert.Null(config);
        Assert.Equal(2, errors.Count);
        Assert.Contains(errors, e => e.Contains("client.security.protocol") && e.Contains("security:"));
        Assert.Contains(errors, e => e.Contains("client.sasl.password") && e.Contains("sasl.password:"));

        errors.Clear();
        config = KafkaConnectionConfig.Parse(Config(new()
        {
            ["bootstrap_servers"] = "b:9092",
            ["client"] = new Dictionary<string, object?> { ["socket.timeout.ms"] = 30000L, ["enable.ssl.certificate.verification"] = false },
        }), errors);

        Assert.Empty(errors);
        Assert.Equal("30000", config!.ClientProperties["socket.timeout.ms"]);
        Assert.Equal("false", config.ClientProperties["enable.ssl.certificate.verification"]);
    }

    [Fact]
    public void Missing_or_invalid_values_are_all_reported()
    {
        var errors = new List<string>();
        var config = KafkaConnectionConfig.Parse(Config(new()
        {
            ["security"] = "sasl_ssl",
            ["idle_timeout"] = 0L,
            ["unknown_key"] = 1L,
        }), errors);

        Assert.Null(config);
        Assert.Contains(errors, e => e.Contains("bootstrap_servers"));
        Assert.Contains(errors, e => e.Contains("sasl:") && e.Contains("sasl_ssl"));
        Assert.Contains(errors, e => e.Contains("idle_timeout"));
        Assert.Contains(errors, e => e.Contains("unknown_key"));
    }

    [Fact]
    public void Client_secret_like_keys_are_redacted()
    {
        var errors = new List<string>();
        var config = KafkaConnectionConfig.Parse(Config(new()
        {
            ["bootstrap_servers"] = "b:9092",
            ["client"] = new Dictionary<string, object?> { ["sasl.oauthbearer.client.secret"] = "oauth-secret" },
        }), errors);

        Assert.Empty(errors);
        Assert.Equal("failed: ***", config!.Redactor.Redact("failed: oauth-secret"));
    }
}
