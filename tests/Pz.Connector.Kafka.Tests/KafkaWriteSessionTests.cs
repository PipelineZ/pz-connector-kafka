using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using Confluent.Kafka;
using Microsoft.Extensions.Logging.Abstractions;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Kafka.Tests;

/// <summary>The write session's decisions that a broker cannot be asked for on demand -- a full
/// local queue, a delivery report that fails, a flush that never drains, and an abort -- driven
/// through a scripted producer instead. Every fact here is deterministic: the fake answers
/// immediately, so nothing waits on wall-clock time.</summary>
public sealed class KafkaWriteSessionTests
{
    private const string Topic = "sink-topic";

    private static readonly Schema Schema = new(
    [
        new Field("id", Int64Type.Default, true),
        new Field("name", StringType.Default, true),
    ], null);

    [Fact]
    public async Task A_full_local_queue_polls_and_re_produces_the_same_row()
    {
        var producer = new FakeProducer { QueueFullBeforeAccepting = 1 };
        await using var session = Session(producer);

        using (var batch = Batch())
        {
            await session.WriteBatchAsync(batch, CancellationToken.None);
        }

        // The row that hit the full queue was re-produced after the poll, not dropped: two rows in,
        // two rows out, and the poll happened between the refusal and the retry.
        Assert.Equal(1, producer.PollCalls);
        Assert.Equal(["1", "2"], producer.Produced.Select(m => Encoding.UTF8.GetString(m.Key)));
        var result = await session.CommitAsync(CancellationToken.None);
        Assert.Equal(2, result.RowsWritten);
        Assert.Equal(1, result.BatchesWritten);
    }

    [Fact]
    public async Task A_failed_delivery_report_surfaces_at_commit_with_the_reports_transience()
    {
        var producer = new FakeProducer { FailDeliveryOfRow = 0, DeliveryError = new Error(ErrorCode.UnknownTopicOrPart, "unknown topic") };
        await using var session = Session(producer);
        using (var batch = Batch())
        {
            // The report lands while the batch is still being written; the session remembers it and
            // reports it at the next boundary rather than half way through a batch.
            await session.WriteBatchAsync(batch, CancellationToken.None);
        }

        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () => await session.CommitAsync(CancellationToken.None));

        Assert.False(ex.IsTransient);
        Assert.Contains(Topic, ex.Message);
        Assert.Contains("UnknownTopicOrPart", ex.Message);
    }

    [Fact]
    public async Task A_flush_that_never_drains_fails_the_commit_transiently()
    {
        var producer = new FakeProducer { OutstandingAfterFlush = 3 };
        // A zero stall budget is one slice: the first flush sets the baseline and the second one,
        // which the fake leaves at the same length, is already the stall.
        await using var session = Session(producer, flushStall: TimeSpan.Zero);
        using (var batch = Batch())
        {
            await session.WriteBatchAsync(batch, CancellationToken.None);
        }

        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () => await session.CommitAsync(CancellationToken.None));

        Assert.True(ex.IsTransient);
        Assert.Contains(Topic, ex.Message);
        Assert.Contains("3 record(s) still unacknowledged", ex.Message);
    }

    [Fact]
    public async Task Abort_settles_what_is_queued_and_the_session_refuses_to_commit()
    {
        var producer = new FakeProducer();
        await using (var session = Session(producer))
        {
            using (var batch = Batch())
            {
                await session.WriteBatchAsync(batch, CancellationToken.None);
            }

            await session.AbortAsync(CancellationToken.None);

            Assert.Equal(1, producer.FlushCalls);
            Assert.Equal(2, producer.Produced.Count);
            await Assert.ThrowsAsync<InvalidOperationException>(async () => await session.CommitAsync(CancellationToken.None));
            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            {
                using var batch = Batch();
                await session.WriteBatchAsync(batch, CancellationToken.None);
            });

            Assert.False(producer.Disposed);
        }

        // The producer is the session's to own: disposing the session releases the librdkafka handle.
        Assert.True(producer.Disposed);
    }

    [Fact]
    public async Task Produced_bytes_are_fresh_arrays_that_outlive_the_engine_batch()
    {
        var producer = new FakeProducer();
        await using var session = Session(producer);

        var batch = Batch();
        await session.WriteBatchAsync(batch, CancellationToken.None);
        // The engine owns the batch again the moment the call returns and disposes it; everything
        // the producer holds must already be the session's own memory.
        batch.Dispose();

        Assert.Equal(2, producer.Produced.Count);
        Assert.NotSame(producer.Produced[0].Key, producer.Produced[1].Key);
        Assert.NotSame(producer.Produced[0].Value, producer.Produced[1].Value);
        Assert.NotSame(producer.Produced[0].Key, producer.Produced[0].Value);
        Assert.Equal("1", Encoding.UTF8.GetString(producer.Produced[0].Key));
        Assert.Equal("2", Encoding.UTF8.GetString(producer.Produced[1].Key));
        Assert.Equal("""{"name":"ann"}""", Encoding.UTF8.GetString(producer.Produced[0].Value));
        Assert.Equal("""{"name":null}""", Encoding.UTF8.GetString(producer.Produced[1].Value));
    }

    private static KafkaWriteSession Session(FakeProducer producer, TimeSpan? flushStall = null)
    {
        var errors = new List<string>();
        var output = KafkaOutputConfig.Parse(
            new OutputSpec("kafka", Topic, "append", "fail_on_change", new Dictionary<string, object?> { ["key"] = "id" }),
            errors);
        Assert.Empty(errors);
        return new KafkaWriteSession(producer, output!, Schema, KafkaRedactor.None, NullLogger.Instance, flushStall);
    }

    private static RecordBatch Batch() => new(Schema,
    [
        new Int64Array.Builder().Append(1).Append(2).Build(),
        new StringArray.Builder().Append("ann").AppendNull().Build(),
    ], 2);

    /// <summary>Answers every producer call the session makes and records what it was handed;
    /// everything else throws, so a session that reached for another Confluent API would fail here
    /// rather than quietly work in one environment only.</summary>
    private sealed class FakeProducer : IProducer<byte[], byte[]>
    {
        private int _produceAttempts;

        /// <summary>How many leading Produce calls are refused with a full local queue before the
        /// fake starts accepting rows.</summary>
        public int QueueFullBeforeAccepting { get; init; }

        /// <summary>Index of the accepted row whose delivery report carries
        /// <see cref="DeliveryError"/>; -1 (the default) delivers every row cleanly.</summary>
        public int FailDeliveryOfRow { get; init; } = -1;

        public Error DeliveryError { get; init; } = new(ErrorCode.NoError);

        /// <summary>What Flush reports as still queued; a non-zero value never shrinks, which is the
        /// stalled producer the commit must give up on.</summary>
        public int OutstandingAfterFlush { get; init; }

        public List<Message<byte[], byte[]>> Produced { get; } = [];

        public int PollCalls { get; private set; }

        public int FlushCalls { get; private set; }

        public bool Disposed { get; private set; }

        public void Produce(string topic, Message<byte[], byte[]> message, Action<DeliveryReport<byte[], byte[]>>? deliveryHandler)
        {
            Assert.Equal(Topic, topic);
            if (_produceAttempts++ < QueueFullBeforeAccepting)
            {
                throw new ProduceException<byte[], byte[]>(
                    new Error(ErrorCode.Local_QueueFull, "queue full"), new DeliveryResult<byte[], byte[]>());
            }

            Produced.Add(message);
            if (Produced.Count - 1 == FailDeliveryOfRow)
            {
                deliveryHandler?.Invoke(new DeliveryReport<byte[], byte[]> { Error = DeliveryError, Message = message });
            }
        }

        public int Poll(TimeSpan timeout)
        {
            PollCalls++;
            return 0;
        }

        public int Flush(TimeSpan timeout)
        {
            FlushCalls++;
            return OutstandingAfterFlush;
        }

        public void Dispose() => Disposed = true;

        public Handle Handle => throw new NotSupportedException();

        public string Name => nameof(FakeProducer);

        public int AddBrokers(string brokers) => throw new NotSupportedException();

        public void SetSaslCredentials(string username, string password) => throw new NotSupportedException();

        public Task<DeliveryResult<byte[], byte[]>> ProduceAsync(string topic, Message<byte[], byte[]> message,
            CancellationToken cancellationToken) => throw new NotSupportedException();

        public Task<DeliveryResult<byte[], byte[]>> ProduceAsync(TopicPartition topicPartition, Message<byte[], byte[]> message,
            CancellationToken cancellationToken) => throw new NotSupportedException();

        public void Produce(TopicPartition topicPartition, Message<byte[], byte[]> message,
            Action<DeliveryReport<byte[], byte[]>>? deliveryHandler) => throw new NotSupportedException();

        public void Flush(CancellationToken cancellationToken) => throw new NotSupportedException();

        public void InitTransactions(TimeSpan timeout) => throw new NotSupportedException();

        public void BeginTransaction() => throw new NotSupportedException();

        public void CommitTransaction(TimeSpan timeout) => throw new NotSupportedException();

        public void CommitTransaction() => throw new NotSupportedException();

        public void AbortTransaction(TimeSpan timeout) => throw new NotSupportedException();

        public void AbortTransaction() => throw new NotSupportedException();

        public void SendOffsetsToTransaction(IEnumerable<TopicPartitionOffset> offsets, IConsumerGroupMetadata groupMetadata,
            TimeSpan timeout) => throw new NotSupportedException();
    }
}
