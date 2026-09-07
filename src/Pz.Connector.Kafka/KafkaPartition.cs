using System.Runtime.CompilerServices;
using Apache.Arrow;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Kafka;

/// <summary>The whole topic as one pz partition. Plan: metadata, watermarks, <see cref="ReadBounds"/>.
/// Read: one assign-only consumer over every unfinished partition, each paused the moment it
/// reaches its bound or reports EOF, until all are done. The token candidate is the map of
/// EFFECTIVE bounds -- what was actually read, not what was planned. EOF below the plan-time bound
/// shortens the token, it never skips: a read_committed consumer stops at the last stable offset
/// while the watermark query reported the high watermark, so an open transaction at plan time
/// leaves [eof, bound) undelivered, and a token carrying the plan-time bound would step over
/// records no run ever landed. The candidate exists only after a completed enumeration: a
/// cancelled or failed read must leave the stored token untouched, so the engine re-reads the
/// same slice next time.</summary>
internal sealed class KafkaPartition(
    KafkaConnectionConfig connection, IKafkaClientFactory factory, KafkaDatasetConfig dataset,
    OffsetToken? token, ILogger logger) : IDatasetPartition, ISyncStatePartition
{
    private static readonly TimeSpan PollInterval = TimeSpan.FromMilliseconds(500);
    private static readonly TimeSpan MetadataTimeout = TimeSpan.FromSeconds(30);

    private string? _candidate;

    public bool TryGetSyncStateCandidate(out string? candidate)
    {
        candidate = _candidate;
        return candidate is not null;
    }

    public async IAsyncEnumerable<RecordBatch> ReadAsync(BatchOptions options, [EnumeratorCancellation] CancellationToken ct)
    {
        _candidate = null;
        var redactor = connection.Redactor;
        var topic = dataset.Topic;
        // librdkafka requires a group.id even for an assign-only consumer that never joins a group.
        var groupId = connection.GroupId ?? "pz-" + Guid.NewGuid().ToString("N");

        IReadOnlyList<PartitionBound> bounds;
        using (var consumer = factory.CreateConsumer(connection.ClientProperties, groupId))
        {
            bounds = await Task.Run(() => Plan(consumer, topic, redactor), ct).ConfigureAwait(false);
        }

        logger.LogDebug("kafka: topic {Topic}: {Partitions} partitions, {Unfinished} with records to read",
            topic, bounds.Count, bounds.Count(b => !b.Done));

        var builder = new EnvelopeBatchBuilder(dataset.Encoding, options, redactor);
        // Seeded with the plan-time bounds (a Done partition keeps its own, having read nothing);
        // the EOF branch below shortens an entry to the offset the read actually stopped at.
        var effective = bounds.ToDictionary(b => b.Partition, b => b.Bound);
        var unfinished = bounds.Where(b => !b.Done).ToDictionary(b => b.Partition);
        var headerScratch = new List<KeyValuePair<string, byte[]?>>();
        if (unfinished.Count > 0)
        {
            using var consumer = factory.CreateConsumer(connection.ClientProperties, groupId);
            try
            {
                consumer.Assign(unfinished.Values.Select(b => new TopicPartitionOffset(topic, b.Partition, new Offset(b.Resume))));
            }
            catch (KafkaException ex)
            {
                throw KafkaErrors.Wrap(ex, redactor, $"topic '{topic}': assigning partitions");
            }

            var idle = TimeSpan.FromSeconds(connection.IdleTimeoutSeconds);
            var lastRecord = Environment.TickCount64;
            while (unfinished.Count > 0)
            {
                ct.ThrowIfCancellationRequested();
                ConsumeResult<byte[], byte[]>? result;
                try
                {
                    result = consumer.Consume(PollInterval);
                }
                catch (KafkaException ex)
                {
                    // KafkaException, not just its ConsumeException subclass: a fetch can fail with
                    // either, and an unwrapped one carries librdkafka's unredacted reason out.
                    throw KafkaErrors.Wrap(ex, redactor, $"topic '{topic}': consuming");
                }

                if (result is null)
                {
                    if (Environment.TickCount64 - lastRecord > idle.TotalMilliseconds)
                    {
                        throw KafkaErrors.Transient(
                            $"topic '{topic}': no record arrived for {connection.IdleTimeoutSeconds}s with " +
                            $"{unfinished.Count} partition(s) still short of their bound; the broker may be unreachable", redactor);
                    }

                    continue;
                }

                lastRecord = Environment.TickCount64;
                var p = result.Partition.Value;
                if (!unfinished.TryGetValue(p, out var bound))
                {
                    continue; // already paused; a record fetched before the pause took effect
                }

                if (result.IsPartitionEOF)
                {
                    // The EOF offset is where the read actually stopped. Clamped to the plan-time
                    // bound because nothing above it was landed either.
                    effective[p] = Math.Min(result.Offset.Value, bound.Bound);
                    Finish(consumer, topic, unfinished, p, redactor);
                    continue;
                }

                if (result.Offset.Value >= bound.Bound)
                {
                    Finish(consumer, topic, unfinished, p, redactor);
                    continue;
                }

                headerScratch.Clear();
                if (result.Message.Headers is { } headers)
                {
                    foreach (var header in headers)
                    {
                        headerScratch.Add(new KeyValuePair<string, byte[]?>(header.Key, header.GetValueBytes()));
                    }
                }

                builder.Append(topic, p, result.Offset.Value, result.Message.Timestamp.UtcDateTime,
                    result.Message.Key, result.Message.Value, headerScratch);
                if (result.Offset.Value + 1 >= bound.Bound)
                {
                    Finish(consumer, topic, unfinished, p, redactor);
                }

                if (builder.TryTakeBatch(out var batch))
                {
                    yield return batch!;
                    // The idle timer measures broker silence, not engine backpressure: the batch
                    // just yielded may be held past idle_timeout, and the first empty poll after
                    // that would otherwise raise an unreachable-broker refusal about a healthy one.
                    lastRecord = Environment.TickCount64;
                }
            }

            try
            {
                consumer.Unassign();
            }
            catch (KafkaException ex)
            {
                throw KafkaErrors.Wrap(ex, redactor, $"topic '{topic}': unassigning partitions");
            }
        }

        if (builder.Flush() is { } last)
        {
            yield return last;
        }

        _candidate = new OffsetToken(topic, effective).Serialize();
        CommitForDashboards(topic, effective, groupId);
    }

    private static void Finish(IConsumer<byte[], byte[]> consumer, string topic,
        Dictionary<int, PartitionBound> unfinished, int partition, KafkaRedactor redactor)
    {
        unfinished.Remove(partition);
        try
        {
            consumer.Pause([new TopicPartition(topic, partition)]);
        }
        catch (KafkaException ex)
        {
            throw KafkaErrors.Wrap(ex, redactor, $"topic '{topic}': pausing partition {partition}");
        }
    }

    private IReadOnlyList<PartitionBound> Plan(IConsumer<byte[], byte[]> consumer, string topic, KafkaRedactor redactor)
    {
        List<int> partitions;
        using (var admin = factory.CreateAdmin(connection.ClientProperties))
        {
            Metadata metadata;
            try
            {
                metadata = admin.GetMetadata(topic, MetadataTimeout);
            }
            catch (KafkaException ex)
            {
                throw KafkaErrors.Wrap(ex, redactor, $"topic '{topic}': fetching metadata");
            }

            var meta = metadata.Topics.FirstOrDefault(t => t.Topic == topic);
            if (meta is null || meta.Error.Code == ErrorCode.UnknownTopicOrPart || meta.Partitions.Count == 0)
            {
                throw KafkaErrors.Fatal($"topic '{topic}' does not exist on the cluster; create it or fix the dataset's `topic:`", redactor);
            }

            if (meta.Error.IsError)
            {
                throw new PzConnectorException(redactor.Redact($"kafka: topic '{topic}': {meta.Error.Code} -- {meta.Error.Reason}"),
                    KafkaErrors.IsTransient(meta.Error.Code));
            }

            partitions = meta.Partitions.Select(p => p.PartitionId).Order().ToList();
        }

        var watermarks = new List<PartitionWatermarks>(partitions.Count);
        try
        {
            foreach (var p in partitions)
            {
                var wm = consumer.QueryWatermarkOffsets(new TopicPartition(topic, p), MetadataTimeout);
                watermarks.Add(new PartitionWatermarks(p, wm.Low.Value, wm.High.Value));
            }
        }
        catch (KafkaException ex)
        {
            throw KafkaErrors.Wrap(ex, redactor, $"topic '{topic}': querying watermarks");
        }

        Dictionary<int, long>? byTimestamp = null;
        if (dataset.Start.Kind == StartKind.Timestamp && (token is null || partitions.Any(p => !token.NextOffsets.ContainsKey(p))))
        {
            try
            {
                var ts = new Timestamp(dataset.Start.Timestamp!.Value.UtcDateTime);
                var resolved = consumer.OffsetsForTimes(partitions.Select(p => new TopicPartitionTimestamp(topic, p, ts)), MetadataTimeout);
                byTimestamp = resolved.ToDictionary(r => r.Partition.Value, r => r.Offset.Value);
            }
            catch (KafkaException ex)
            {
                throw KafkaErrors.Wrap(ex, redactor, $"topic '{topic}': resolving `start:` timestamp");
            }
        }

        return ReadBounds.Compute(topic, token, dataset.Start, watermarks, byTimestamp, redactor);
    }

    /// <summary>Best effort, only when the user configured a group: the landed read must never fail
    /// because a lag dashboard's bookkeeping did.</summary>
    private void CommitForDashboards(string topic, IReadOnlyDictionary<int, long> effective, string groupId)
    {
        if (connection.GroupId is null)
        {
            return;
        }

        try
        {
            using var consumer = factory.CreateConsumer(connection.ClientProperties, groupId);
            consumer.Commit(effective.Select(e => new TopicPartitionOffset(topic, e.Key, new Offset(e.Value))));
        }
        catch (KafkaException ex)
        {
            logger.LogWarning("kafka: topic {Topic}: committing offsets to group failed: {Code}", topic, ex.Error.Code);
        }
    }
}
