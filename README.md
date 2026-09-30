# flink-projections

Stateful Flink projections of entity streams into documents: joins, grouping and deletes, with state you control.

You have entities arriving as change streams (candidates, their experiences, their skills) and you need one document per candidate in a search index, kept correct as any part changes or disappears. flink-projections is a set of DataStream operators for exactly that pipeline:

**entity streams → keyed join → group into document → upsert or delete downstream**

A search index is the flagship target, but a projection can feed any sink: a Kafka topic, a cache, a document store.

## Why not Flink SQL?

SQL can express the same joins, but it owns the state. A query change or a Flink upgrade can re-plan the job and leave your savepoint unrestorable, state TTL is a planner hint rather than part of your operator's contract, delete handling follows the planner's changelog rules rather than your domain's, and debugging means reading generated code. flink-projections trades SQL's brevity for control:

- **Stable operator ids and state names**, treated as a compatibility contract across releases.
- **Explicit delete semantics**: removing a child entity updates the document; removing the root deletes it.
- **Per-stream state lifecycle**, with each TTL set by you.
- **Convergence under replay**: at-least-once delivery and idempotent upserts keyed by document id.
- **Plain operators** you wire into your own job: `Changes`, `Nest` and `Lookup`.

## Modules

| Artifact | What it is |
|---|---|
| `flink-projections` | The operators: `Changes`, `Nest`, `Lookup` |
| `flink-projections-kafka` | Kafka topics in as change streams, documents out as upserts and tombstones |
| `flink-projections-elasticsearch` | Documents into an Elasticsearch 8 or 9 index: index and delete by document id |
| `flink-projections-examples` | A runnable resume-search job; not published |

## Dependencies

Flink is `provided`: your job owns its version. Nothing is shaded or bundled.

`flink-projections-kafka` also treats `flink-connector-kafka` as `provided`. Declare it in your job, at the build that matches your Flink version (`5.0.0-2.2` for Flink 2.2). The module has no Avro, Schema Registry or JSON dependency; you pass Flink's own `DeserializationSchema` and `SerializationSchema`.

`flink-projections-elasticsearch` treats `co.elastic.clients:elasticsearch-java` as `provided`. Declare an 8.19.x client in your job; it talks to Elasticsearch 8 and, through REST compatibility headers, to Elasticsearch 9. It is tested with client 8.19.22 against Elasticsearch 8.19.22 and 9.5.3. The module has no Jackson dependency: documents are the bytes your `SerializationSchema` produces, sent as they are.

## Kafka

```java
DataStream<Change<Candidate>> candidates = KafkaChanges.from(
        "candidates",
        env,
        KafkaSourceOptions.builder()
                .bootstrapServers("kafka:9092")
                .topic("resume.candidates")
                .groupId("resume-search")
                .build(),
        new JsonDeserializationSchema<>(Candidate.class),
        Candidate::id,
        DeleteDetection.flag(Candidate::deleted),
        TypeInformation.of(Candidate.class));

KafkaChanges.to(
        "candidate-docs",
        documents,
        KafkaSinkOptions.builder()
                .bootstrapServers("kafka:9092")
                .topic("resume.candidate-docs")
                .build(),
        new JsonSerializationSchema<>());
```

- Input records are soft deletes: a record whose flag is set becomes a `Delete` carrying that record as the entity's last value. A null-value tombstone on an input topic fails the job with `TombstoneNotSupportedException`.
- Output records are keyed by document id (UTF-8). An upsert writes the document; a delete writes a tombstone, so a compacted topic keeps one record per live document.
- Each document's upserts and tombstones keep their order when the sink is fed directly by the operator that emits the documents. Do not rebalance or change parallelism between them.
- With `DeliveryGuarantee.EXACTLY_ONCE`, set `property("transaction.timeout.ms", …)` no higher than the broker's `transaction.max.timeout.ms` (15 minutes by default); the connector's own default is one hour.
- `flink-connector-base` ships with the Flink distribution; when you run a job from an IDE or a test, put it on the classpath yourself.
- Operator ids are `kafka_source_<name>`, then `changes_<name>`, and `kafka_sink_<name>`.
- Sources emit no watermarks. The library never creates topics or checks that they exist; a missing topic fails when the job runs.

| Option | Default |
|---|---|
| `KafkaSourceOptions.bootstrapServers`, `topic`, `groupId` | required |
| `KafkaSourceOptions.startingOffsets` | `OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST)` |
| `KafkaSourceOptions.property` | none; `bootstrap.servers` and `group.id` are rejected |
| `KafkaSinkOptions.bootstrapServers`, `topic` | required |
| `KafkaSinkOptions.deliveryGuarantee` | `DeliveryGuarantee.AT_LEAST_ONCE` |
| `KafkaSinkOptions.transactionalIdPrefix` | required with `EXACTLY_ONCE`, rejected otherwise |
| `KafkaSinkOptions.property` | none; `bootstrap.servers` and `transactional.id` are rejected |

Avro with Confluent Schema Registry is a format you bring, for example from `flink-avro-confluent-registry`:

```java
DeserializationSchema<GenericRecord> candidates =
        ConfluentRegistryAvroDeserializationSchema.forGeneric(candidateSchema, "http://registry:8081");
```

The example module maps `GenericRecord` to its own records in `AvroRecordReader` and `AvroRecordWriter`.

## Elasticsearch

```java
ElasticsearchChanges.to(
        "candidate-docs",
        documents,
        ElasticsearchSinkOptions.builder()
                .hosts("https://es.example.com:9200")
                .index("candidate-docs")
                .auth(ElasticsearchAuth.apiKey(System.getenv("ES_API_KEY")))
                .build(),
        new JsonSerializationSchema<>());
```

- An `Upsert` indexes the whole document under `_id` = document id; a `Delete` deletes that `_id`. Both are idempotent: a delete of a missing document, or of a document in a missing index, succeeds.
- The sink keys the stream by document id before writing, so every change for a document goes to the same writer whatever the sink's parallelism. The writer keeps only the newest change per id and sends one bulk request at a time, synchronously, so a retry of the same request cannot overtake a newer change. The one residual case is a bulk that the client gives up on after its socket timeout, which the cluster can still apply later. The sink asks Elasticsearch to give up after 20 seconds, below the client's 30-second socket timeout, to make that unlikely. Changes carry no version, so the case is not fully excluded.
- A bulk is sent when `maxBatchActions` or `maxBatchBytes` is reached, every `flushInterval`, and on every checkpoint. Delivery is at-least-once with no writer state: after a checkpoint completes, everything before it is acknowledged by Elasticsearch. After a restore the replay re-applies changes idempotently; until it catches up, a document can briefly show an older version.
- Failures with status 429, 502, 503 or 504, whether of a single item or of a whole bulk request, are retried, and so are requests that cannot reach the cluster, with exponential backoff up to `maxRetries`. Any other failure, such as a mapping conflict, a missing index for an upsert with auto-create off, or 401/403, fails the job with `ElasticsearchWriteException` naming the index, the document id, the error and how to fix it.
- The library never creates indices or mappings; create them, or rely on Elasticsearch's auto-create.
- TLS uses the JVM truststore. Credentials go in `auth(...)`, never in the host URL. Credentials passed to `auth(...)` are serialized into the job graph like any other option, so read them from a secret store or environment variable at job submission.
- The operator id is `elasticsearch_sink_<name>`. The name must be lowercase letters, digits and `-`, starting with a letter.

| Option | Default |
|---|---|
| `hosts`, `index` | required; hosts are `http` or `https` URLs without path |
| `auth` | `ElasticsearchAuth.none()`; also `basic(username, password)` and `apiKey(encodedApiKey)` |
| `maxBatchActions` | `1000` |
| `maxBatchBytes` | `5242880` (5 MiB) |
| `flushInterval` | `Duration.ofSeconds(1)` |
| `maxRetries` | `8` |
| `retryBackoff` | `Duration.ofMillis(100)` doubling to `Duration.ofSeconds(10)` |

## Example

`flink-projections-examples` runs the resume-search projection: candidates, experiences, projects, skills and companies from five topics, joined with `Lookup` and two `Nest` levels, into one document per candidate on `resume.candidate-docs`.

`ResumeSearchJob.main` takes `--bootstrap-servers`, `--format json|avro` (default `json`) and, for Avro, `--schema-registry-url`. `--output kafka|elasticsearch` (default `kafka`) picks where documents go: the `resume.candidate-docs` topic, or the index `--elasticsearch-index` (default `candidate-docs`) on `--elasticsearch-hosts` (comma-separated URLs), always as JSON. The module is not shaded; to run it on a cluster, build a job jar that bundles it with its dependencies. `mvn verify -P integration-tests` runs the same job against Kafka, Schema Registry and Elasticsearch in Docker.
