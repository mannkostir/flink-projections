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
| `flink-projections-examples` | A runnable resume-search job; not published |

## Dependencies

Flink is `provided`: your job owns its version. Nothing is shaded or bundled.

`flink-projections-kafka` also treats `flink-connector-kafka` as `provided`. Declare it in your job, at the build that matches your Flink version (`5.0.0-2.2` for Flink 2.2). The module has no Avro, Schema Registry or JSON dependency; you pass Flink's own `DeserializationSchema` and `SerializationSchema`.

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

## Example

`flink-projections-examples` runs the resume-search projection: candidates, experiences, projects, skills and companies from five topics, joined with `Lookup` and two `Nest` levels, into one document per candidate on `resume.candidate-docs`.

`ResumeSearchJob.main` takes `--bootstrap-servers`, `--format json|avro` (default `json`) and, for Avro, `--schema-registry-url`. The module is not shaded; to run it on a cluster, build a job jar that bundles it with its dependencies. `mvn verify -P integration-tests` runs the same job against Kafka and Schema Registry in Docker.
