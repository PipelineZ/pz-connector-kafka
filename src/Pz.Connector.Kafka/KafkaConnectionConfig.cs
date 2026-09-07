using System.Globalization;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Kafka;

/// <summary>The typed connection surface, flattened into the librdkafka property map every client is
/// built from. A <c>client:</c> key that a typed key already sets is refused rather than merged --
/// one place per setting keeps a credential or protocol from being silently overridden -- and every
/// value that is a credential is registered with the redactor by content.</summary>
internal sealed record KafkaConnectionConfig(
    IReadOnlyDictionary<string, string> ClientProperties,
    int IdleTimeoutSeconds,
    string? GroupId,
    KafkaRedactor Redactor)
{
    private static readonly string[] KnownKeys =
        ["bootstrap_servers", "security", "sasl", "ssl", "client_id", "idle_timeout", "group_id", "client", "base_dir"];
    private static readonly string[] Securities = ["plaintext", "ssl", "sasl_plaintext", "sasl_ssl"];
    private static readonly string[] Mechanisms = ["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512", "OAUTHBEARER"];

    /// <summary>typed YAML key path → librdkafka property. Used both to map and to name the typed
    /// key in an overlap refusal.</summary>
    private static readonly (string Yaml, string Prop)[] TypedProperties =
    [
        ("bootstrap_servers", "bootstrap.servers"), ("security", "security.protocol"),
        ("sasl.mechanism", "sasl.mechanism"), ("sasl.username", "sasl.username"), ("sasl.password", "sasl.password"),
        ("ssl.ca_location", "ssl.ca.location"), ("ssl.certificate_location", "ssl.certificate.location"),
        ("ssl.key_location", "ssl.key.location"), ("ssl.key_password", "ssl.key.password"),
        ("client_id", "client.id"), ("group_id", "group.id"),
    ];

    public static KafkaConnectionConfig? Parse(ConnectorConfig config, List<string> errors)
    {
        var start = errors.Count;
        var props = new Dictionary<string, string>(StringComparer.Ordinal);
        var secrets = new List<string>();

        foreach (var key in config.Values.Keys.Where(k => !KnownKeys.Contains(k, StringComparer.Ordinal)))
        {
            errors.Add($"unknown connection key '{key}'; known keys: {string.Join(", ", KnownKeys)}");
        }

        var bootstrap = config.GetString("bootstrap_servers");
        if (string.IsNullOrWhiteSpace(bootstrap))
        {
            errors.Add("'bootstrap_servers' is required (host:port, comma-separated)");
        }
        else
        {
            props["bootstrap.servers"] = bootstrap;
        }

        var security = config.GetString("security") ?? "plaintext";
        if (!Securities.Contains(security, StringComparer.Ordinal))
        {
            errors.Add($"'security' must be one of {string.Join(", ", Securities)}; got '{security}'");
        }

        props["security.protocol"] = security;

        var sasl = Section(config, "sasl", errors);
        if (security.StartsWith("sasl_", StringComparison.Ordinal))
        {
            if (sasl is null)
            {
                errors.Add($"'sasl:' (mechanism, username, password) is required when security is '{security}'");
            }
            else
            {
                var mechanism = Str(sasl, "mechanism");
                if (mechanism is null || !Mechanisms.Contains(mechanism, StringComparer.Ordinal))
                {
                    errors.Add($"'sasl.mechanism' must be one of {string.Join(", ", Mechanisms)}");
                }
                else
                {
                    props["sasl.mechanism"] = mechanism;
                }

                Require(sasl, "username", "sasl.username", props, errors, secret: false, secrets);
                Require(sasl, "password", "sasl.password", props, errors, secret: true, secrets);
            }
        }
        else if (sasl is not null)
        {
            errors.Add($"'sasl:' is only valid when security is sasl_plaintext or sasl_ssl; got '{security}'");
        }

        var baseDir = config.GetString("base_dir");
        var ssl = Section(config, "ssl", errors);
        if (ssl is not null)
        {
            Optional(ssl, "ca_location", "ssl.ca.location", props, baseDir);
            Optional(ssl, "certificate_location", "ssl.certificate.location", props, baseDir);
            Optional(ssl, "key_location", "ssl.key.location", props, baseDir);
            if (Str(ssl, "key_password") is { Length: > 0 } keyPassword)
            {
                props["ssl.key.password"] = keyPassword;
                secrets.Add(keyPassword);
            }

            foreach (var key in ssl.Keys.Where(k => k is not ("ca_location" or "certificate_location" or "key_location" or "key_password")))
            {
                errors.Add($"unknown 'ssl.{key}'; known: ca_location, certificate_location, key_location, key_password");
            }
        }

        props["client.id"] = config.GetString("client_id") is { Length: > 0 } clientId ? clientId : "pz";

        var idle = 60;
        if (config.Values.ContainsKey("idle_timeout"))
        {
            var value = config.GetInt("idle_timeout");
            if (value is null or < 1)
            {
                errors.Add("'idle_timeout' must be a positive integer number of seconds");
            }
            else
            {
                idle = (int)Math.Min(value.Value, int.MaxValue);
            }
        }

        var groupId = config.GetString("group_id");
        if (!string.IsNullOrEmpty(groupId))
        {
            props["group.id"] = groupId;
        }
        else
        {
            groupId = null;
        }

        var client = Section(config, "client", errors);
        if (client is not null)
        {
            foreach (var (key, raw) in client)
            {
                var typed = TypedProperties.FirstOrDefault(t => t.Prop == key);
                if (typed.Prop is not null)
                {
                    errors.Add($"'client.{key}' duplicates '{typed.Yaml}:'; set it in one place");
                    continue;
                }

                var value = raw switch
                {
                    null => null,
                    bool b => b ? "true" : "false",
                    IFormattable f => f.ToString(null, CultureInfo.InvariantCulture),
                    _ => raw.ToString(),
                };
                if (value is null)
                {
                    errors.Add($"'client.{key}' has no value");
                    continue;
                }

                props[key] = value;
                if (key.Contains("password", StringComparison.OrdinalIgnoreCase)
                    || key.Contains("secret", StringComparison.OrdinalIgnoreCase)
                    || key.Contains("token", StringComparison.OrdinalIgnoreCase))
                {
                    secrets.Add(value);
                }
            }
        }

        return errors.Count == start
            ? new KafkaConnectionConfig(props, idle, groupId, new KafkaRedactor(secrets))
            : null;
    }

    private static IReadOnlyDictionary<string, object?>? Section(ConnectorConfig config, string key, List<string> errors)
    {
        if (!config.Values.TryGetValue(key, out var raw) || raw is null)
        {
            return null;
        }

        if (raw is IReadOnlyDictionary<string, object?> ro)
        {
            return ro;
        }

        if (raw is IDictionary<string, object?> rw)
        {
            return new Dictionary<string, object?>(rw, StringComparer.Ordinal);
        }

        errors.Add($"'{key}:' must be a mapping");
        return null;
    }

    private static string? Str(IReadOnlyDictionary<string, object?> section, string key) =>
        section.TryGetValue(key, out var v) ? v?.ToString() : null;

    private static void Require(IReadOnlyDictionary<string, object?> section, string key, string prop,
        Dictionary<string, string> props, List<string> errors, bool secret, List<string> secrets)
    {
        var value = Str(section, key);
        if (string.IsNullOrEmpty(value))
        {
            errors.Add($"'sasl.{key}' is required");
            return;
        }

        props[prop] = value;
        if (secret)
        {
            secrets.Add(value);
        }
    }

    private static void Optional(IReadOnlyDictionary<string, object?> section, string key, string prop,
        Dictionary<string, string> props, string? baseDir)
    {
        var value = Str(section, key);
        if (string.IsNullOrEmpty(value))
        {
            return;
        }

        props[prop] = baseDir is not null && !Path.IsPathRooted(value) ? Path.Combine(baseDir, value) : value;
    }
}
