# flink-projections

Stateful Flink projections of entity streams into documents: joins, grouping and deletes, with state you control.

You have entities arriving as change streams (candidates, their experiences, their skills) and you need one document per candidate in a search index, kept correct as any part changes or disappears. flink-projections is a set of DataStream operators for exactly that pipeline:

**entity streams → keyed join → group into document → upsert or delete downstream**

A search index is the flagship target, but a projection can feed any sink: a Kafka topic, a cache, a document store.

## Why not Flink SQL?

SQL can express the same joins, but it owns the state. A query change or a Flink upgrade can re-plan the job and leave your savepoint unrestorable, state TTL is a planner hint rather than part of your operator's contract, delete handling follows the planner's changelog rules rather than your domain's, and debugging means reading generated code. flink-projections trades SQL's brevity for control:

- **Stable operator ids and state names**, treated as a compatibility contract across releases.
- **Explicit delete semantics**: removing a child entity updates the document; removing the root deletes it (planned).
- **Per-stream state lifecycle**, with each TTL set by you (planned).
- **Convergence under replay**: at-least-once delivery and idempotent upserts keyed by document id.
- **Plain operators** you wire into your own job, with optional `Job` scaffolding for the batteries-included path.

## Dependencies

Flink, Kafka and Avro are `provided`: your job owns their versions. Nothing is shaded or bundled. The Confluent Schema Registry client is currently a compile dependency; making it optional is planned.
