using System.Text;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Kafka.Tests;

/// <summary>The five rules that decide where a partition read stops and what the token then
/// carries, driven through the client-factory seam with scripted clients instead of a broker. A
/// broker cannot be asked for these boundaries on demand -- a record delivered exactly at the
/// bound, an EOF strictly below it, a broker that answers nothing -- so they are scripted here and
/// left to the docker facts to confirm end to end.</summary>
public sealed class KafkaPartitionLoopTests
{
    private const string Topic = "loop-topic";

    [Fact]
    public async Task A_record_at_the_bound_is_discarded_and_the_partition_paused()
    {
        var factory = Factory(high: 2);
        // Offset 1 is never delivered (an aborted transaction's marker occupies it), so the read
        // reaches the bound by being handed the record that sits on it.
        factory.Consumer.Script.Enqueue(Record(0, offset: 0, "a"));
        factory.Consumer.Script.Enqueue(Record(0, offset: 2, "past-the-bound"));

        var (rows, token) = await KafkaSourceBehaviorTests.ReadAsync(Connector(factory), Config(), Spec());

        Assert.Equal(["a"], rows.Select(r => r.Value));
        Assert.Equal(new TopicPartition(Topic, 0), Assert.Single(factory.Consumer.Paused));
        Assert.Equal($$$"""{"v":1,"topic":"{{{Topic}}}","partitions":{"0":2}}""", token);
    }

    [Fact]
    public async Task Reaching_the_bound_pauses_the_partition_and_the_token_carries_the_bound()
    {
        var factory = Factory(high: 2);
        factory.Consumer.Script.Enqueue(Record(0, offset: 0, "a"));
        factory.Consumer.Script.Enqueue(Record(0, offset: 1, "b"));

        var (rows, token) = await KafkaSourceBehaviorTests.ReadAsync(Connector(factory), Config(), Spec());

        Assert.Equal(["a", "b"], rows.Select(r => r.Value));
        // The whole clean cycle: assigned at the resume offset, paused on reaching the bound,
        // unassigned once the last partition finished.
        Assert.Equal(new TopicPartitionOffset(Topic, 0, new Offset(0)), Assert.Single(factory.Consumer.Assigned));
        Assert.Equal(new TopicPartition(Topic, 0), Assert.Single(factory.Consumer.Paused));
        Assert.Equal(1, factory.Consumer.UnassignCount);
        Assert.Equal($$$"""{"v":1,"topic":"{{{Topic}}}","partitions":{"0":2}}""", token);
    }

    [Fact]
    public async Task An_eof_below_the_bound_shortens_the_token_to_the_eof_offset()
    {
        var factory = Factory(high: 5);
        factory.Consumer.Script.Enqueue(Record(0, offset: 0, "a"));
        factory.Consumer.Script.Enqueue(Eof(0, offset: 3));

        var (rows, token) = await KafkaSourceBehaviorTests.ReadAsync(Connector(factory), Config(), Spec());

        Assert.Equal(["a"], rows.Select(r => r.Value));
        Assert.Equal($$$"""{"v":1,"topic":"{{{Topic}}}","partitions":{"0":3}}""", token);
    }

    [Fact]
    public async Task A_silent_broker_past_the_idle_timeout_is_transient()
    {
        var factory = Factory(high: 1);
        // Nothing scripted: the consumer answers null, and only after the configured idle timeout
        // has elapsed, which is the condition under test rather than a wait for something else.
        factory.Consumer.SilenceBeforeNull = TimeSpan.FromMilliseconds(1_200);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            () => KafkaSourceBehaviorTests.ReadAsync(Connector(factory), Config(idleTimeoutSeconds: 1), Spec()));

        Assert.True(ex.IsTransient);
        Assert.Contains("no record arrived for 1s", ex.Message);
    }

    [Fact]
    public async Task A_cancelled_enumeration_leaves_no_candidate()
    {
        var factory = Factory(high: 4);
        for (var offset = 0; offset < 4; offset++)
        {
            factory.Consumer.Script.Enqueue(Record(0, offset, $"r{offset}"));
        }

        await using var source = await Connector(factory).OpenAsync(Config(), CancellationToken.None);
        var partition = Assert.Single(await source.PlanReadAsync(Spec(), ReadHints.None, CancellationToken.None));
        using var cts = new CancellationTokenSource();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await foreach (var batch in partition.ReadAsync(new BatchOptions(TargetBatchBytes: 64, MaxRowsPerBatch: 1), cts.Token))
            {
                batch.Dispose();
                await cts.CancelAsync();
            }
        });

        Assert.False(((ISyncStatePartition)partition).TryGetSyncStateCandidate(out var token));
        Assert.Null(token);
    }

    [Fact]
    public async Task A_client_property_the_builder_rejects_fails_the_read_without_echoing_the_secret()
    {
        var factory = Factory(high: 1);
        // How librdkafka refuses a `client:` value it cannot parse: out of the builder, not out of
        // the first call, with the offending value quoted back in the message.
        factory.CreateConsumerFailure = new ArgumentException("bad value for x: hunter2");
        var config = new ConnectorConfig(new Dictionary<string, object?>
        {
            ["bootstrap_servers"] = "kafka.invalid:9092",
            // Registered as a secret by the name heuristic, so the redactor knows the value.
            ["client"] = new Dictionary<string, object?> { ["sasl.oauthbearer.client.secret"] = "hunter2" },
        });

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            () => KafkaSourceBehaviorTests.ReadAsync(Connector(factory), config, Spec()));

        Assert.False(ex.IsTransient);
        Assert.DoesNotContain("hunter2", ex.Message);
        Assert.Contains("building the kafka client", ex.Message);
    }

    [Fact]
    public async Task A_resume_offset_retention_dropped_mid_read_fails_non_transiently_with_the_retention_wording()
    {
        var factory = Factory(high: 3);
        factory.Consumer.Script.Enqueue(Record(0, offset: 0, "a"));
        // How librdkafka reports a fetch below the low watermark under auto.offset.reset=error.
        factory.Consumer.ConsumeFailure = new ConsumeException(
            new ConsumeResult<byte[], byte[]> { Topic = Topic, Partition = new Partition(0), Offset = new Offset(1) },
            new Error(ErrorCode.Local_AutoOffsetReset, "fetch failed due to requested offset not available on the broker: Broker: Offset out of range"));

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            () => KafkaSourceBehaviorTests.ReadAsync(Connector(factory), Config(), Spec()));

        Assert.False(ex.IsTransient);
        Assert.Contains("partition 0", ex.Message);
        Assert.Contains("dropped by retention", ex.Message);
        Assert.Contains("--full-refresh", ex.Message);
    }

    [Fact]
    public async Task A_group_commit_that_never_answers_is_abandoned_after_the_idle_timeout_and_the_token_stands()
    {
        var factory = Factory(high: 1);
        factory.Consumer.Script.Enqueue(Record(0, offset: 0, "a"));
        // The commit blocks until the fact releases it, standing in for a coordinator lookup that
        // librdkafka retries without end; the read must come back on the idle budget, not on it.
        using var gate = new ManualResetEventSlim(false);
        factory.Consumer.CommitGate = gate;
        var values = new Dictionary<string, object?>
        {
            ["bootstrap_servers"] = "kafka.invalid:9092",
            ["group_id"] = "dashboards",
            ["idle_timeout"] = 1L,
        };

        try
        {
            var (rows, token) = await KafkaSourceBehaviorTests.ReadAsync(Connector(factory), new ConnectorConfig(values), Spec());

            Assert.Equal(["a"], rows.Select(r => r.Value));
            Assert.Equal($$$"""{"v":1,"topic":"{{{Topic}}}","partitions":{"0":1}}""", token);
            Assert.Equal(1, factory.Consumer.CommitsStarted);
        }
        finally
        {
            gate.Set();
        }
    }

    private static ISourceConnector Connector(FakeFactory factory) => new KafkaConnector(loggerFactory: null, factory);

    private static DatasetSpec Spec() => new("kafka", Topic, new Dictionary<string, object?>());

    private static ConnectorConfig Config(int? idleTimeoutSeconds = null)
    {
        var values = new Dictionary<string, object?> { ["bootstrap_servers"] = "kafka.invalid:9092" };
        if (idleTimeoutSeconds is { } seconds)
        {
            values["idle_timeout"] = (long)seconds;
        }

        return new ConnectorConfig(values);
    }

    /// <summary>One topic, one partition, offsets [0, <paramref name="high"/>).</summary>
    private static FakeFactory Factory(long high)
    {
        var factory = new FakeFactory();
        factory.Consumer.Watermarks[0] = new WatermarkOffsets(new Offset(0), new Offset(high));
        return factory;
    }

    private static ConsumeResult<byte[], byte[]> Record(int partition, long offset, string value) => new()
    {
        Topic = Topic,
        Partition = new Partition(partition),
        Offset = new Offset(offset),
        Message = new Message<byte[], byte[]>
        {
            Key = null!,
            Value = Encoding.UTF8.GetBytes(value),
            // Fixed per offset so a batch's timestamp column is reproducible run to run.
            Timestamp = new Timestamp(DateTime.UnixEpoch.AddSeconds(offset), TimestampType.CreateTime),
            Headers = null!,
        },
    };

    /// <summary>An end-of-partition marker: librdkafka delivers these with no message at all, which
    /// is why the loop must decide on one before it touches <c>Message</c>.</summary>
    private static ConsumeResult<byte[], byte[]> Eof(int partition, long offset) => new()
    {
        Topic = Topic,
        Partition = new Partition(partition),
        Offset = new Offset(offset),
        IsPartitionEOF = true,
    };

    /// <summary>Hands out one consumer for every request. The read creates a consumer to plan with,
    /// disposes it, then creates the one it consumes on, so the fake's Dispose is a no-op and the
    /// facts can read the pauses off a single object.</summary>
    private sealed class FakeFactory : IKafkaClientFactory
    {
        public FakeConsumer Consumer { get; } = new();

        /// <summary>Set to make every CreateConsumer throw, standing in for a librdkafka builder
        /// that rejects a property.</summary>
        public Exception? CreateConsumerFailure { get; set; }

        public IConsumer<byte[], byte[]> CreateConsumer(IReadOnlyDictionary<string, string> properties, string groupId) =>
            CreateConsumerFailure is { } failure ? throw failure : Consumer;

        public IProducer<byte[], byte[]> CreateProducer(IReadOnlyDictionary<string, string> properties, string compression) =>
            throw new NotSupportedException();

        public IAdminClient CreateAdmin(IReadOnlyDictionary<string, string> properties) =>
            new FakeAdmin(Consumer.Watermarks.Keys.Order().ToList());
    }

    private sealed class FakeConsumer : IConsumer<byte[], byte[]>
    {
        public Dictionary<int, WatermarkOffsets> Watermarks { get; } = [];

        public Queue<ConsumeResult<byte[], byte[]>> Script { get; } = [];

        public List<TopicPartition> Paused { get; } = [];

        public List<TopicPartitionOffset> Assigned { get; } = [];

        public TimeSpan SilenceBeforeNull { get; set; }

        /// <summary>Thrown by the first Consume call once the script is exhausted.</summary>
        public Exception? ConsumeFailure { get; set; }

        /// <summary>When set, every Commit blocks on it; <see cref="CommitsStarted"/> counts the calls.</summary>
        public ManualResetEventSlim? CommitGate { get; set; }

        public int CommitsStarted => Volatile.Read(ref _commitsStarted);

        private int _commitsStarted;

        public int UnassignCount { get; private set; }

        public ConsumeResult<byte[], byte[]>? Consume(TimeSpan timeout)
        {
            if (Script.Count > 0)
            {
                return Script.Dequeue();
            }

            if (ConsumeFailure is { } failure)
            {
                ConsumeFailure = null;
                throw failure;
            }

            Thread.Sleep(SilenceBeforeNull);
            return null;
        }

        public WatermarkOffsets QueryWatermarkOffsets(TopicPartition topicPartition, TimeSpan timeout) =>
            Watermarks[topicPartition.Partition.Value];

        public void Assign(IEnumerable<TopicPartitionOffset> partitions) => Assigned.AddRange(partitions);

        public void Pause(IEnumerable<TopicPartition> partitions) => Paused.AddRange(partitions);

        public void Unassign() => UnassignCount++;

        public void Dispose()
        {
        }

        public Handle Handle => throw new NotSupportedException();

        public string Name => nameof(FakeConsumer);

        public string MemberId => throw new NotSupportedException();

        public List<TopicPartition> Assignment => throw new NotSupportedException();

        public List<string> Subscription => throw new NotSupportedException();

        public IConsumerGroupMetadata ConsumerGroupMetadata => throw new NotSupportedException();

        public int AddBrokers(string brokers) => throw new NotSupportedException();

        public void SetSaslCredentials(string username, string password) => throw new NotSupportedException();

        public ConsumeResult<byte[], byte[]> Consume(int millisecondsTimeout) => throw new NotSupportedException();

        public ConsumeResult<byte[], byte[]> Consume(CancellationToken cancellationToken) => throw new NotSupportedException();

        public void Subscribe(IEnumerable<string> topics) => throw new NotSupportedException();

        public void Subscribe(string topic) => throw new NotSupportedException();

        public void Unsubscribe() => throw new NotSupportedException();

        public void Assign(TopicPartition partition) => throw new NotSupportedException();

        public void Assign(TopicPartitionOffset partition) => throw new NotSupportedException();

        public void Assign(IEnumerable<TopicPartition> partitions) => throw new NotSupportedException();

        public void IncrementalAssign(IEnumerable<TopicPartitionOffset> partitions) => throw new NotSupportedException();

        public void IncrementalAssign(IEnumerable<TopicPartition> partitions) => throw new NotSupportedException();

        public void IncrementalUnassign(IEnumerable<TopicPartition> partitions) => throw new NotSupportedException();

        public void StoreOffset(ConsumeResult<byte[], byte[]> result) => throw new NotSupportedException();

        public void StoreOffset(TopicPartitionOffset offset) => throw new NotSupportedException();

        public List<TopicPartitionOffset> Commit() => throw new NotSupportedException();

        public void Commit(IEnumerable<TopicPartitionOffset> offsets)
        {
            Interlocked.Increment(ref _commitsStarted);
            CommitGate?.Wait();
        }

        public void Commit(ConsumeResult<byte[], byte[]> result) => throw new NotSupportedException();

        public void Seek(TopicPartitionOffset tpo) => throw new NotSupportedException();

        public void Resume(IEnumerable<TopicPartition> partitions) => throw new NotSupportedException();

        public List<TopicPartitionOffset> Committed(TimeSpan timeout) => throw new NotSupportedException();

        public List<TopicPartitionOffset> Committed(IEnumerable<TopicPartition> partitions, TimeSpan timeout) =>
            throw new NotSupportedException();

        public Offset Position(TopicPartition partition) => throw new NotSupportedException();

        public List<TopicPartitionOffset> OffsetsForTimes(IEnumerable<TopicPartitionTimestamp> timestampsToSearch, TimeSpan timeout) =>
            throw new NotSupportedException();

        public WatermarkOffsets GetWatermarkOffsets(TopicPartition topicPartition) => throw new NotSupportedException();

        public void Close() => throw new NotSupportedException();
    }

    private sealed class FakeAdmin(IReadOnlyList<int> partitions) : IAdminClient
    {
        public Metadata GetMetadata(string topic, TimeSpan timeout) => new(
            [new BrokerMetadata(1, "kafka.invalid", 9092)],
            [
                new TopicMetadata(topic,
                    partitions.Select(p => new PartitionMetadata(p, 1, [1], [1], new Error(ErrorCode.NoError))).ToList(),
                    new Error(ErrorCode.NoError)),
            ],
            1,
            "kafka.invalid:9092");

        public void Dispose()
        {
        }

        public Handle Handle => throw new NotSupportedException();

        public string Name => nameof(FakeAdmin);

        public int AddBrokers(string brokers) => throw new NotSupportedException();

        public void SetSaslCredentials(string username, string password) => throw new NotSupportedException();

        public Metadata GetMetadata(TimeSpan timeout) => throw new NotSupportedException();

        public List<GroupInfo> ListGroups(TimeSpan timeout) => throw new NotSupportedException();

        public GroupInfo ListGroup(string group, TimeSpan timeout) => throw new NotSupportedException();

        public Task CreatePartitionsAsync(IEnumerable<PartitionsSpecification> partitionsSpecifications,
            CreatePartitionsOptions options) => throw new NotSupportedException();

        public Task DeleteGroupsAsync(IList<string> groups, DeleteGroupsOptions options) => throw new NotSupportedException();

        public Task DeleteTopicsAsync(IEnumerable<string> topics, DeleteTopicsOptions options) => throw new NotSupportedException();

        public Task CreateTopicsAsync(IEnumerable<TopicSpecification> topics, CreateTopicsOptions options) =>
            throw new NotSupportedException();

        public Task AlterConfigsAsync(Dictionary<ConfigResource, List<ConfigEntry>> configs, AlterConfigsOptions options) =>
            throw new NotSupportedException();

        public Task<List<IncrementalAlterConfigsResult>> IncrementalAlterConfigsAsync(
            Dictionary<ConfigResource, List<ConfigEntry>> configs, IncrementalAlterConfigsOptions options) =>
            throw new NotSupportedException();

        public Task<List<DescribeConfigsResult>> DescribeConfigsAsync(IEnumerable<ConfigResource> resources,
            DescribeConfigsOptions options) => throw new NotSupportedException();

        public Task<List<DeleteRecordsResult>> DeleteRecordsAsync(IEnumerable<TopicPartitionOffset> topicPartitionOffsets,
            DeleteRecordsOptions options) => throw new NotSupportedException();

        public Task CreateAclsAsync(IEnumerable<AclBinding> aclBindings, CreateAclsOptions options) =>
            throw new NotSupportedException();

        public Task<DescribeAclsResult> DescribeAclsAsync(AclBindingFilter aclBindingFilter, DescribeAclsOptions options) =>
            throw new NotSupportedException();

        public Task<List<DeleteAclsResult>> DeleteAclsAsync(IEnumerable<AclBindingFilter> aclBindingFilters,
            DeleteAclsOptions options) => throw new NotSupportedException();

        public Task<DeleteConsumerGroupOffsetsResult> DeleteConsumerGroupOffsetsAsync(string group,
            IEnumerable<TopicPartition> partitions, DeleteConsumerGroupOffsetsOptions options) => throw new NotSupportedException();

        public Task<List<AlterConsumerGroupOffsetsResult>> AlterConsumerGroupOffsetsAsync(
            IEnumerable<ConsumerGroupTopicPartitionOffsets> groupPartitions, AlterConsumerGroupOffsetsOptions options) =>
            throw new NotSupportedException();

        public Task<List<ListConsumerGroupOffsetsResult>> ListConsumerGroupOffsetsAsync(
            IEnumerable<ConsumerGroupTopicPartitions> groupPartitions, ListConsumerGroupOffsetsOptions options) =>
            throw new NotSupportedException();

        public Task<ListConsumerGroupsResult> ListConsumerGroupsAsync(ListConsumerGroupsOptions options) =>
            throw new NotSupportedException();

        public Task<DescribeConsumerGroupsResult> DescribeConsumerGroupsAsync(IEnumerable<string> groups,
            DescribeConsumerGroupsOptions options) => throw new NotSupportedException();

        public Task<DescribeUserScramCredentialsResult> DescribeUserScramCredentialsAsync(IEnumerable<string> users,
            DescribeUserScramCredentialsOptions options) => throw new NotSupportedException();

        public Task AlterUserScramCredentialsAsync(IEnumerable<UserScramCredentialAlteration> alterations,
            AlterUserScramCredentialsOptions options) => throw new NotSupportedException();
    }
}
