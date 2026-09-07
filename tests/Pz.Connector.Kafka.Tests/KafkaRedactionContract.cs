using Pz.Connectors.TestKit;

namespace Pz.Connector.Kafka.Tests;

/// <summary>The TestKit's credential shapes through this connector's redactor, seeded with the same
/// synthetic secret the suite embeds, exactly as a real config would seed it with the SASL password.</summary>
public sealed class KafkaRedactionContract : ErrorRedactionContractTests
{
    protected override string RedactErrorText(string thirdPartyMessage) =>
        new KafkaRedactor(["pz-testkit-secret-value"]).Redact(thirdPartyMessage);
}
