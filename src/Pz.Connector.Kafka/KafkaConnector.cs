using System.Reflection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Kafka;

/// <summary>Apache Kafka for pz: a topic is a feed-shaped dataset resumed from per-partition offsets
/// the engine stores as the dataset's sync-state token; a sink output is an append-only produce.
/// Capabilities: <see cref="ConnectorCapabilities.SyncState"/> only -- one token per dataset means
/// one pz partition per dataset, and there is no SQL fragment DuckDB could scan a broker with.</summary>
public sealed class KafkaConnector(ILoggerFactory? loggerFactory = null) : IConnector, ISourceConnector, ISinkConnector
{
    private readonly ILoggerFactory _loggerFactory = loggerFactory ?? NullLoggerFactory.Instance;

    public ConnectorInfo Info { get; } = new(
        "kafka",
        typeof(KafkaConnector).Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion
            ?? "0.0.0",
        ProtocolVersion.Major);

    public ConnectorCapabilities Capabilities => ConnectorCapabilities.SyncState;

    public string ConnectionConfigSchema => """
        { "type": "object", "required": ["bootstrap_servers"], "properties": {
            "bootstrap_servers": { "type": "string" },
            "security": { "enum": ["plaintext", "ssl", "sasl_plaintext", "sasl_ssl"] },
            "sasl": { "type": "object", "required": ["mechanism", "username", "password"], "properties": {
                "mechanism": { "enum": ["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512", "OAUTHBEARER"] },
                "username": { "type": "string" }, "password": { "type": "string" } },
              "additionalProperties": false },
            "ssl": { "type": "object", "properties": {
                "ca_location": { "type": "string" }, "certificate_location": { "type": "string" },
                "key_location": { "type": "string" }, "key_password": { "type": "string" } },
              "additionalProperties": false },
            "client_id": { "type": "string" },
            "idle_timeout": { "type": "integer", "minimum": 1 },
            "group_id": { "type": "string" },
            "client": { "type": "object", "additionalProperties": { "type": ["string", "number", "boolean"] } },
            "base_dir": { "type": "string" } },
          "additionalProperties": false }
        """;

    public string DatasetConfigSchema => """
        { "type": "object", "properties": {
            "topic": { "type": "string" },
            "start": { "type": "string" },
            "encoding": { "enum": ["utf8", "base64"] } },
          "additionalProperties": false }
        """;

    public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct) =>
        ValueTask.FromResult(ValidationResult.Success);

    public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct) =>
        ValueTask.FromResult(new ConnectionCheck(false, "not implemented yet"));

    ValueTask<ISource> ISourceConnector.OpenAsync(ConnectorConfig config, CancellationToken ct) =>
        throw new NotImplementedException();

    ValueTask<ISink> ISinkConnector.OpenAsync(ConnectorConfig config, CancellationToken ct) =>
        throw new NotImplementedException();
}
