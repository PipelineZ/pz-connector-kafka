using System.Diagnostics;

namespace Pz.Connector.Kafka.Tests;

/// <summary>Docker probe shared by every broker-backed fact. A collection fixture must not throw
/// SkipException (xunit reports that as a failed fixture, not a skip), so the fixture consults
/// <see cref="IsAvailable"/> and no-ops, and each test class constructor calls
/// <see cref="SkipUnlessDocker"/> where the skip does register as a skip.</summary>
internal static class DockerFacts
{
    private static readonly Lazy<bool> DockerAvailable = new(() =>
    {
        try
        {
            var psi = new ProcessStartInfo("docker", "info") { RedirectStandardOutput = true, RedirectStandardError = true };
            using var process = Process.Start(psi);
            return process is not null && process.WaitForExit(5000) && process.ExitCode == 0;
        }
        catch
        {
            return false;
        }
    });

    public static bool IsAvailable => DockerAvailable.Value;

    public static void SkipUnlessDocker() => Skip.IfNot(DockerAvailable.Value, "docker is not available");
}
