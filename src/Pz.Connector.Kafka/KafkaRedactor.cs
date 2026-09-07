using System.Text.RegularExpressions;

namespace Pz.Connector.Kafka;

/// <summary>Strips credentials from any text that may reach a PzConnectorException message, a log
/// line, or a ConnectionCheck: every configured secret value is replaced wherever it occurs (a
/// broker error can echo it in any position, not only beside its key), and the password-bearing
/// librdkafka property names are rewritten even when the value is not one of ours (a property dump
/// in an error string). Secrets shorter than 3 characters are not matched -- replacing them would
/// shred unrelated text.</summary>
internal sealed partial class KafkaRedactor
{
    public const string Mask = "***";

    public static readonly KafkaRedactor None = new([]);

    private readonly string[] _secrets;

    public KafkaRedactor(IReadOnlyList<string> secrets)
    {
        _secrets = secrets.Where(s => s.Length >= 3).Distinct(StringComparer.Ordinal)
            .OrderByDescending(s => s.Length).ToArray();
    }

    public string Redact(string text)
    {
        if (string.IsNullOrEmpty(text))
        {
            return text;
        }

        foreach (var secret in _secrets)
        {
            text = text.Replace(secret, Mask, StringComparison.Ordinal);
        }

        return PasswordProperty().Replace(text, m => $"{m.Groups["key"].Value}={Mask}");
    }

    // sasl.password=value / ssl.key.password="quoted value" as librdkafka prints them. The unquoted
    // branch excludes ';' and ',' so a "key=value; key2=value2" property dump doesn't get its
    // separator swallowed into the match.
    [GeneratedRegex("""(?<key>\b(?:sasl\.password|ssl\.key\.password|ssl\.keystore\.password|sasl\.oauthbearer\.client\.secret))=(?:"[^"]*"|[^\s;,]+)""")]
    private static partial Regex PasswordProperty();
}
