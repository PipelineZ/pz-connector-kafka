namespace Pz.Connector.Kafka;

internal sealed record PartitionWatermarks(int Partition, long Low, long High);

/// <summary>What one run reads from one partition: [Resume, Bound). Bound is the high watermark
/// at plan time, which is what makes a run finite and a retry re-read the same slice.</summary>
internal sealed record PartitionBound(int Partition, long Resume, long Bound)
{
    public bool Done => Resume >= Bound;
}

/// <summary>Pure resume/bound arithmetic. A stored offset outside [low, high] is an error, never a
/// silent skip: below low means retention already dropped records the dataset never landed; above
/// high means the topic's offsets were reset under the stored state. Every partition's problem is
/// reported in one message.</summary>
internal static class ReadBounds
{
    public static IReadOnlyList<PartitionBound> Compute(
        string topic, OffsetToken? token, StartPosition start, IReadOnlyList<PartitionWatermarks> watermarks,
        IReadOnlyDictionary<int, long>? timestampOffsets, KafkaRedactor redactor)
    {
        var bounds = new List<PartitionBound>(watermarks.Count);
        var problems = new List<string>();
        foreach (var wm in watermarks.OrderBy(w => w.Partition))
        {
            long resume;
            if (token is not null && token.NextOffsets.TryGetValue(wm.Partition, out var stored))
            {
                if (stored < wm.Low)
                {
                    problems.Add($"partition {wm.Partition}: stored offset {stored} is below the earliest available {wm.Low}, " +
                                 "so records were dropped by retention before this run landed them");
                    continue;
                }

                if (stored > wm.High)
                {
                    problems.Add($"partition {wm.Partition}: stored offset {stored} is beyond the high watermark {wm.High}, " +
                                 "so the topic's offsets were reset under the stored state");
                    continue;
                }

                resume = stored;
            }
            else
            {
                resume = start.Kind switch
                {
                    StartKind.Earliest => wm.Low,
                    StartKind.Latest => wm.High,
                    _ => timestampOffsets is not null && timestampOffsets.TryGetValue(wm.Partition, out var at) && at >= 0
                        ? Math.Clamp(at, wm.Low, wm.High)
                        : wm.High,
                };
            }

            bounds.Add(new PartitionBound(wm.Partition, resume, wm.High));
        }

        if (problems.Count > 0)
        {
            throw KafkaErrors.Fatal(
                $"topic '{topic}' cannot resume from the stored sync state: {string.Join("; ", problems)}. " +
                "Run with --full-refresh to start from `start:` again, or edit the dataset's state with `pz state`", redactor);
        }

        return bounds;
    }
}
