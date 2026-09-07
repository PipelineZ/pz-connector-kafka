using Pz.Connectors.Abstractions;

namespace Pz.Connector.Kafka.Tests;

public sealed class KafkaConnectorTests
{
    [Fact]
    public void Identity_is_kafka_on_the_current_protocol_major()
    {
        var connector = new KafkaConnector();

        Assert.Equal("kafka", connector.Info.Name);
        Assert.Equal(ProtocolVersion.Major, connector.Info.ProtocolMajor);
        Assert.False(string.IsNullOrWhiteSpace(connector.Info.Version));
    }

    [Fact]
    public void Declares_exactly_sync_state()
    {
        Assert.Equal(ConnectorCapabilities.SyncState, new KafkaConnector().Capabilities);
    }

    [Fact]
    public void Config_schemas_are_json_objects()
    {
        var connector = new KafkaConnector();
        using var connection = System.Text.Json.JsonDocument.Parse(connector.ConnectionConfigSchema);
        using var dataset = System.Text.Json.JsonDocument.Parse(connector.DatasetConfigSchema);

        Assert.Equal("object", connection.RootElement.GetProperty("type").GetString());
        Assert.Equal("object", dataset.RootElement.GetProperty("type").GetString());
    }

    [Fact]
    public async Task Validate_reports_every_config_error()
    {
        var result = await new KafkaConnector().ValidateAsync(
            new ConnectorConfig(new Dictionary<string, object?> { ["security"] = "nope", ["idle_timeout"] = -1L }),
            CancellationToken.None);

        Assert.False(result.IsValid);
        Assert.Equal(3, result.Errors.Count); // bootstrap_servers, security, idle_timeout
    }
}
