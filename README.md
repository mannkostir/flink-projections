# flink-projections

[![Maven Central](https://img.shields.io/maven-central/v/io.github.mannkostir/flink-projections)](https://central.sonatype.com/artifact/io.github.mannkostir/flink-projections)
[![CI](https://github.com/mannkostir/flink-projections/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/mannkostir/flink-projections/actions/workflows/ci.yml)

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

## Installation

```xml
<dependency>
    <groupId>io.github.mannkostir</groupId>
    <artifactId>flink-projections</artifactId>
    <version>0.1.0</version>
</dependency>
```

Add `flink-projections-kafka` or `flink-projections-elasticsearch`, at the same version, for the adapters. Flink 2.2.1 is the only version tested so far. Java 17 or later.

## Dependencies

Flink is `provided`: your job owns its version. Nothing is shaded or bundled.

`flink-projections-kafka` also treats `flink-connector-kafka` as `provided`. Declare it in your job, at the build that matches your Flink version (`5.0.0-2.2` for Flink 2.2). The module has no Avro, Schema Registry or JSON dependency; you pass Flink's own `DeserializationSchema` and `SerializationSchema`.

`flink-projections-elasticsearch` treats `co.elastic.clients:elasticsearch-java` as `provided`. Declare an 8.19.x client in your job; it talks to Elasticsearch 8 and, through REST compatibility headers, to Elasticsearch 9. It is tested with client 8.19.22 against Elasticsearch 8.19.22 and 9.5.3. The module has no Jackson dependency: documents are the bytes your `SerializationSchema` produces, sent as they are.

## Core

```java
DataStream<Change<Experience>> experiences = Changes.from(
        "experiences", experienceRows, Experience::id, Experience::deleted, EXPERIENCE);

DataStream<Change<Experience>> withCompany = Lookup.of("company", experiences, Experience::companyId, EXPERIENCE)
        .from(companies, COMPANY)
        .enrich((experience, company) -> experience.withCompanyName(company == null ? null : company.name()), EXPERIENCE);

Nest<Experience> experienceLevel = Nest.parent("experience", withCompany, EXPERIENCE);
ChildSlot<Project> projectSlot = experienceLevel.child("projects", projects, Project::experienceId, PROJECT);
DataStream<Change<ExperienceDoc>> experienceDocs = experienceLevel.assemble(
        (experience, children) -> new ExperienceDoc(
                experience.id(),
                experience.candidateId(),
                experience.title(),
                experience.companyName(),
                children.get(projectSlot).stream().map(Project::name).toList()),
        EXPERIENCE_DOC);

Nest<Candidate> candidateLevel = Nest.parent("candidate", candidates, CANDIDATE)
        .withOptions(NestOptions.builder().orphanTimeout(Duration.ofHours(1)).build());
ChildSlot<ExperienceDoc> experienceSlot = candidateLevel.child("experiences", experienceDocs, ExperienceDoc::candidateId, EXPERIENCE_DOC);
ChildSlot<Skill> skillSlot = candidateLevel.child("skills", skills, Skill::candidateId, SKILL);
DataStream<Change<CandidateDoc>> documents = candidateLevel.assemble(
        (candidate, children) -> new CandidateDoc(
                candidate.id(),
                candidate.name(),
                children.get(experienceSlot),
                children.get(skillSlot).stream().map(Skill::name).toList()),
        CANDIDATE_DOC);
```

- `Changes.from` turns a stream into `Upsert`s and `Delete`s by id; a `Delete` carries the flagged record as the entity's last value.
- `Lookup` attaches one dimension to every entity whose lookup key matches, and re-emits those entities when the dimension changes. One `Lookup` enriches once.
- `Nest` keys a parent and its child slots by parent id and emits one assembled document per parent. Levels chain bottom-up: one level's documents are the next level's children.
- Deleting a child re-emits its parent's document without it; deleting a parent emits a `Delete` of its document and discards its children; a parent re-created later assembles without them until each child changes again.
- When an entity's parent key or lookup key changes, it is removed from the old key and added under the new one. A `Delete` is routed by the entity's stored last value, so it reaches the key the entity is actually stored under.
- Changes for one entity must arrive in order, as they do from a source partitioned by entity id. Documents converge; there is no ordering across entities.
- State TTL is refreshed on create and write, so a parent or dimension that rarely changes can expire while its children stay active. Expiry emits no `Delete`.
- With `requireMatch(true)`, an entity that moves from a key with a dimension to a key without one keeps its last enriched value downstream until that key gets a dimension or the entity changes again.

| Option | Default |
|---|---|
| `NestOptions.parentStateTtl` | none: parent and last-document state live until the parent is deleted, which also discards its children |
| `NestOptions.orphanTimeout` | none: children whose parent never arrives are kept; when set, they are cleared between one and two timeouts (processing time) after the last orphaned child upsert if the parent is still absent, together with all other children at that key; each key holds at most one pending orphan timer, though timers restored from a 0.1.0 savepoint still fire at their original times |
| `ChildOptions.stateTtl` | none: applies to the slot's child state and its routing state |
| `LookupOptions.stateTtl` | none: applies to entity, dimension and routing state together |
| `LookupOptions.requireMatch` | `false`: an entity without a dimension is emitted with `null` as the dimension; `true` emits nothing until a dimension exists and deletes enriched entities when it is removed |

Names given to `Changes`, `Nest`, child slots and `Lookup` must be lowercase letters, digits and `-`, starting with a letter.

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
- `KafkaChanges.to` and `ElasticsearchChanges.to` return the `DataStreamSink`, so you can set its parallelism or slot sharing group. Keep its uid.
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

## Compatibility contract

Operator uids and state names come only from the names you give, never from class names. They are how a savepoint finds its state, and they change only in a major version. Renaming one of your names, not upgrading the library, is what breaks a savepoint.

| Operator uid | Created by |
|---|---|
| `changes_<name>` | `Changes.from`, and `KafkaChanges.from` after its source |
| `nest_<name>` | `Nest.assemble` |
| `nest_<name>_route_<slot>` | `Nest.assemble`, once per child slot |
| `lookup_<name>` | `Lookup…enrich` |
| `lookup_<name>_route` | `Lookup…enrich`, routing the entities |
| `kafka_source_<name>` | `KafkaChanges.from` |
| `kafka_sink_<name>` | `KafkaChanges.to` |
| `elasticsearch_sink_<name>` | `ElasticsearchChanges.to` |

| State name | Holds |
|---|---|
| `<name>.parent` | a `Nest` level's parent value |
| `<name>.child.<slot>` | a `Nest` level's children in one slot |
| `<name>.last-doc` | a `Nest` level's last emitted document |
| `<name>.route.<slot>.last` | the last value of each child, for relocation |
| `<name>.entities` | a `Lookup`'s entities under one key |
| `<name>.dimension` | a `Lookup`'s dimension under one key |
| `<name>.route.last` | the last value of each `Lookup` entity, for relocation |

The Kafka source keeps its offsets in the connector's own state under `kafka_source_<name>`. The Kafka sink's state, used only with `EXACTLY_ONCE`, lives under uids derived from `kafka_sink_<name>`. The Elasticsearch sink has no writer state.

### Public API

Every public type in `flink-projections`, `flink-projections-kafka` and `flink-projections-elasticsearch` is checked by [japicmp](https://siom79.github.io/japicmp/) during `mvn verify` against the release named by `api.baseline.version` in the root `pom.xml`. A binary or source incompatible change fails the build and names the class or method it breaks; additions pass.

An intended break, allowed only in a major version, is accepted by naming it in the japicmp `<excludes>` of the root `pom.xml`:

```xml
<excludes>
  <exclude>io.github.mannkostir.projections.NestOptions$Builder#parentStateTtl(java.time.Duration)</exclude>
</excludes>
```

After each release, `api.baseline.version` moves to that release and the excludes are cleared.

## Example

`flink-projections-examples` runs the resume-search projection: candidates, experiences, projects, skills and companies from five topics, joined with `Lookup` and two `Nest` levels, into one document per candidate on `resume.candidate-docs`.

`ResumeSearchJob.main` takes `--bootstrap-servers`, `--format json|avro` (default `json`) and, for Avro, `--schema-registry-url`. `--output kafka|elasticsearch` (default `kafka`) picks where documents go: the `resume.candidate-docs` topic, or the index `--elasticsearch-index` (default `candidate-docs`) on `--elasticsearch-hosts` (comma-separated URLs), always as JSON. The module is not shaded; to run it on a cluster, build a job jar that bundles it with its dependencies. `mvn verify -P integration-tests` runs the same job against Kafka, Schema Registry and Elasticsearch in Docker.

## License

Apache License 2.0; see [LICENSE](LICENSE).
