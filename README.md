# Pz.Connector.Kafka

Apache Kafka source and sink for [PipelineZ](https://pipelinez.dev) (`pz`), served out of process.
A topic reads as a **feed**: every run lands the records between the offsets stored from the last
run and each partition's high watermark at the start of this one, and hands `pz` the new offsets as
the dataset's sync-state token. A sink output **appends**: one produced record per row.

## Installation

```yaml
# project.yml
connectors:
  - package: Pz.Connector.Kafka
    version: 0.1.0
```

`pz restore` installs the binary for your platform (linux-x64, linux-arm64, osx-arm64, win-x64) and
`pz run` spawns it. The package is self-contained (no .NET runtime needed); it is not Native AOT
because the Kafka client library does not support it.

## Connection

```yaml
# connections.yml
events:
  connector: kafka
  bootstrap_servers: ${KAFKA_BOOTSTRAP}
  security: sasl_ssl                 # plaintext (default) | ssl | sasl_plaintext | sasl_ssl
  sasl:
    mechanism: PLAIN                 # PLAIN | SCRAM-SHA-256 | SCRAM-SHA-512 | OAUTHBEARER
    username: ${KAFKA_API_KEY}
    password: ${KAFKA_API_SECRET}
  ssl:                               # optional; relative paths resolve against the project directory
    ca_location: certs/ca.pem
    certificate_location: certs/client.pem
    key_location: certs/client.key
    key_password: ${KAFKA_KEY_PASSWORD}
  client_id: pz                      # optional
  idle_timeout: 60                   # optional, seconds of silence before a read or a commit fails transiently
  group_id: pz-orders                # optional; offsets are committed here after each read, for lag dashboards only
  client:                            # optional escape hatch: raw librdkafka properties
    socket.timeout.ms: 30000
```

`idle_timeout` bounds silence in both directions: a read whose broker delivers no record for that
long fails transiently, and so does a sink commit whose producer out-queue stops shrinking for that
long. The best-effort `group_id` commit after a read is given the same budget, then abandoned with
a warning; the read's stored offsets never depend on it.

A `client:` key that a typed key already sets (`security.protocol`, `sasl.*`, `ssl.*.location`,
`client.id`, `group.id`, `bootstrap.servers`) is refused: set each thing in one place. Passwords,
and any `client:` key containing `password`, `secret`, or `token`, are redacted from every error.
Confluent Cloud is `security: sasl_ssl` with `mechanism: PLAIN` and the API key/secret.

## Reading a topic

```yaml
  entities:
    orders:
      read:
        topic: orders            # optional; defaults to the entity name
        start: earliest          # earliest (default) | latest | 2026-01-01T00:00:00Z
        encoding: utf8           # utf8 (default) | base64
```

Every record lands as one row:

| column | type | |
|---|---|---|
| `topic` | varchar | |
| `partition` | integer | |
| `offset` | bigint | |
| `timestamp` | timestamp (UTC) | the record's timestamp |
| `key` | varchar, nullable | text, or base64 of the raw bytes with `encoding: base64` |
| `value` | varchar, nullable | text, or base64 |
| `headers` | varchar | JSON object `name → value`; a repeated name becomes an array |

Decode in SQL: `json_extract_string(value, '$.customer')`. With `encoding: utf8` a key or value that
is not valid UTF-8 fails the read, naming the record; switch that dataset to `base64`.

`start:` applies only when no offsets are stored for a partition: the first run, `--full-refresh`,
or a partition added since. A stored offset that retention has already dropped fails the run
(non-transient) rather than skipping records; recover with `pz run --full-refresh` or edit the
dataset's state with `pz state`.

A feed dataset paired with an `append` output needs `duplicates: accept` on that output
(`PZ0214`): a retried run can re-deliver a slice.

## Writing a topic

```yaml
  entities:
    order_events:
      write:
        topic: order-events      # optional; defaults to the entity name
        strategy: append         # the only strategy kafka supports
        key: order_id            # optional column (varchar, integer, or bigint)
        value: payload           # optional varchar column sent verbatim; omit for whole-row JSON
        headers: [source]        # optional columns sent as string headers
        compression: zstd        # none (default) | gzip | snappy | lz4 | zstd
```

Without `value:`, each row becomes a JSON object of every column not named in `key:` or
`headers:`: integers and doubles as numbers, decimals as strings, booleans, dates as
`yyyy-MM-dd`, timestamps as `yyyy-MM-ddTHH:mm:ss.ffffffZ`, nulls as `null`. The producer is
idempotent with `acks=all`; commit flushes and fails if any record was not acknowledged. Delivery
across runs is at-least-once, as for every `append` output. The topic must already exist.

A row whose `value:` column is null produces a record with a null value -- on a compacted topic that
is a tombstone, deleting the key, not a skipped row. Without `value:`, the whole-row JSON carries
only pz's type matrix -- varchar, integer, bigint, double, decimal, boolean, date, timestamp -- and a
column of any other type is refused when the write starts, before a record is produced, naming the
column: name a `value:` column, or drop it from the pipeline's projection.

## Development

```bash
dotnet build Pz.Connector.Kafka.slnx -c Release
dotnet test Pz.Connector.Kafka.slnx -c Release --no-build                                  # broker facts need docker; they SKIP without it
dotnet publish src/Pz.Connector.Kafka -c Release -r linux-x64 -p:PzPackaging=self-contained # stage the host RID
dotnet pack src/Pz.Connector.Kafka -c Release -o packages -p:PzPackaging=self-contained     # nupkg with pz.connector.json
```

`-p:PzPackaging=self-contained` works around a package-props evaluation-order gap in
`Pz.Connectors.Sdk` 0.5.0 (its `PublishAot`/`PublishSingleFile` defaults are set from
`PzPackaging`-conditioned property groups that run before this project's own `<PzPackaging>` element
is read); passing it as a command-line global property makes it win. Drop this once a fixed SDK
release reads `PzPackaging` correctly from the project file alone. On a cold NuGet cache, restore the
RID explicitly first (`dotnet restore src/Pz.Connector.Kafka -r linux-x64 -p:PzPackaging=self-contained`)
before publishing with `--no-restore`; `publish -r` alone can skip pulling the RID-specific
`Microsoft.NETCore.App.Runtime`/`Microsoft.AspNetCore.App.Runtime` packs this self-contained build's
`FrameworkReference` needs (NETSDK1112).

Releases are tag-triggered (`v*`) and publish to nuget.org through trusted publishing.
