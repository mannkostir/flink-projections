# Operator-first API on Flink 2.x

## Goal

Replace the current `Job`-centred code with a small operator-first API. The API builds one document per root entity from a tree of entity change streams. It supports nesting to any depth, deletes that cascade, per-stream state TTL, and `uid`s and state names that the user controls. It targets Flink 2.x and is fully test-covered, including savepoint round-trips.

This is spec 1 of four. The others are:

2. The resume-search end-to-end example plus a `projections-kafka` module.
3. A `projections-elasticsearch` module.
4. The first Maven Central release.

## Model

A document is a tree assembled bottom-up. Each level joins one parent entity with its child entities, keyed by the parent id. The output of one level is a child input of the level above. For the resume-search model:

- projects → experience, keyed by `experience_id`;
- company data is attached to each experience by a lookup;
- experiences and skills → candidate, keyed by `candidate_id`;
- the candidate document is the final output.

## Public API

Everything public lives in the package `io.github.mannkostir.projections`. Operators, routing steps, serializers and rule classes in that package are package-private. There are no other packages.

### Change stream

```java
public sealed interface Change<T> permits Upsert, Delete {
    String id();
    T value();
}
public record Upsert<T>(String id, T value) implements Change<T> {}
public record Delete<T>(String id, T value) implements Change<T> {}
```

Rules for every `Change`:

- `id` is non-null and not blank.
- `value` is non-null. A `Delete` carries the last known value of the entity, because routing a delete to its parent needs the value.
- The constructors reject violations with `IllegalArgumentException`, and the message names the offending field.

`Changes` adapts a user stream:

```java
public static <T> DataStream<Change<T>> from(
        String name,
        DataStream<T> stream,
        KeySelector<T, String> id,
        FilterFunction<T> isDeleted,
        TypeInformation<T> type)
```

- Records for which `isDeleted` is true become `Delete`; all others become `Upsert`.
- Operator `uid` and name: `changes_<name>`.
- `Changes.typeInfo(TypeInformation<T>)` returns the `TypeInformation<Change<T>>` described under Serialization.

### Nest

```java
public static <P> Nest<P> parent(String name, DataStream<Change<P>> parents, TypeInformation<P> type)
public Nest<P> withOptions(NestOptions options)
public <C> ChildSlot<C> child(String slot, DataStream<Change<C>> children, KeySelector<C, String> parentKey, TypeInformation<C> type)
public <C> ChildSlot<C> child(String slot, DataStream<Change<C>> children, KeySelector<C, String> parentKey, TypeInformation<C> type, ChildOptions options)
public <O> DataStream<Change<O>> assemble(Assembler<P, O> assembler, TypeInformation<O> type)
```

```java
public interface Assembler<P, O> extends Serializable {
    O assemble(P parent, Children children) throws Exception;
}
public final class Children {
    public <C> List<C> get(ChildSlot<C> slot);
}
```

- `ChildSlot<C>` is an opaque, typed handle. `Children.get` returns an unmodifiable list sorted by child `id`.
- A level's output `Change<O>` uses the parent's `id`.
- A level has one or more child slots. Internally, all child streams are unioned into one tagged stream, so each level is a single keyed two-input operator.

### Lookup

```java
public static <E> Lookup<E> of(String name, DataStream<Change<E>> entities, KeySelector<E, String> lookupKey, TypeInformation<E> type)
public Lookup<E> withOptions(LookupOptions options)
public <D> Lookup.WithDimension<E, D> from(DataStream<Change<D>> dimensions, TypeInformation<D> type)
public <O> DataStream<Change<O>> enrich(Enricher<E, D, O> enricher, TypeInformation<O> type)
```

```java
public interface Enricher<E, D, O> extends Serializable {
    O enrich(E entity, D dimension) throws Exception;
}
```

- Dimensions are keyed by their `Change` id, and entities are keyed by `lookupKey`.
- The output uses the entity's `id`.
- `dimension` is `null` when no dimension is present for the key.

### Options

These are typed builders with stated defaults, validated in `build()`.

| Type | Option | Default |
|---|---|---|
| `NestOptions` | `parentStateTtl(Duration)` | none |
| `NestOptions` | `orphanTimeout(Duration)` | none |
| `ChildOptions` | `stateTtl(Duration)` | none |
| `LookupOptions` | `stateTtl(Duration)` | none |
| `LookupOptions` | `requireMatch(boolean)` | `false` |

- A `Duration` must be positive.
- TTL uses Flink `StateTtlConfig`, with update on create and write and never returning expired state.

### Names are the savepoint contract

- **Name format:** names and slot names must match `[a-z][a-z0-9-]*`.
- **Uniqueness:** slot names must be unique within a level.
- **Violations:** a bad name, `assemble` called on a level with no child slot, or `assemble` called twice fails at graph-build time with `ProjectionConfigurationException`. The message says how to fix the configuration.

| Component | `uid` and name | State descriptor names |
|---|---|---|
| `Changes` | `changes_<name>` | none |
| Nest level | `nest_<name>` | `<name>.parent`, `<name>.child.<slot>`, `<name>.last-doc` |
| Nest routing, per slot | `nest_<name>_route_<slot>` | `<name>.route.<slot>.last` |
| Lookup | `lookup_<name>` | `<name>.entities`, `<name>.dimension` |
| Lookup routing | `lookup_<name>_route` | `<name>.route.last` |

Class names never contribute to a `uid` or a state name.

## Semantics

### Routing step (re-parenting)

Every `Nest` child slot and every `Lookup` entity input passes through a routing step keyed by the entity `id`. The step keeps the entity's last value.

- **`Upsert` whose target key differs from the last value's key** (the target key is the parent key for `Nest`, or the lookup key for `Lookup`): emit a relocation `Delete(id, lastValue)` first, which routes to the old key, then the `Upsert`, which routes to the new key.
- **What a relocation does:**
  - For `Nest`, a relocation is an ordinary child `Delete` at the old parent.
  - For `Lookup`, a relocation only removes the entity from the old key's state and emits nothing. Otherwise two parallel `Lookup` instances would each emit output for the same entity id, and Flink does not order those.
- **Any other `Upsert`:** forward it and store the value.
- **`Delete`:** forward it and clear the stored value. When a last value is stored, the forwarded `Delete` carries that last value instead of its own, so it is routed to the key the entity is actually stored under even if the delete's own value changed or nulled that key.

### Nest, per parent key

| Event | State change | Emission |
|---|---|---|
| Child `Upsert`, parent absent | store child | none |
| Child `Upsert`, parent present | store child | `Upsert(parentId, assembled)` |
| Child `Delete` of a stored child, parent present | remove child | `Upsert(parentId, assembled)` |
| Child `Delete` of an unknown child | none | none |
| Child `Delete` of a stored child, parent absent | remove child | none |
| Parent `Upsert` | store parent | `Upsert(parentId, assembled)` |
| Parent `Delete`, parent present | clear parent, all child slots and last document | `Delete(parentId, last assembled document)` |
| Parent `Delete`, parent absent | clear all child slots | none |

Further rules:

- **Last document:** `<name>.last-doc` holds the last emitted document, so a parent `Delete` can carry it as its value.
- **No empty-document emissions:** a child delete never produces an empty document or a `Delete`.
- **Orphan timeout:** when `orphanTimeout` is set, storing a child while the parent is absent registers a processing-time timer at now plus the timeout. When the timer fires and the parent is still absent, all child slots for the key are cleared, with no emission.

### Lookup, per lookup key

| Event | `requireMatch(false)` | `requireMatch(true)` |
|---|---|---|
| Entity `Upsert` | store; emit `Upsert(enrich(entity, dimension or null))` | store; emit only if a dimension is present |
| Entity `Delete` of a stored entity | remove; emit `Delete(id, enrich(storedEntity, dimension or null))` | remove; emit `Delete` only if a dimension is present |
| Entity `Delete` of an unknown entity | none | none |
| Entity relocation (lookup key changed) | remove from the old key; emit nothing | remove from the old key; emit nothing |
| Dimension `Upsert` | store; re-emit `Upsert` for every stored entity | store; emit `Upsert` for every stored entity |
| Dimension `Delete` | clear dimension; re-emit every entity enriched with `null` | clear dimension; emit `Delete(id, enrich(entity, oldDimension))` for every stored entity |

Entities are never dropped because a dimension changes.

### Failures

- An exception thrown by `Assembler`, `Enricher`, a `KeySelector` or a `FilterFunction` fails the job. Nothing is caught and continued.
- Per-record policies (skip, dead-letter) are out of scope and will be a Strategy family later.

### Ordering assumption

- Changes for one entity arrive in order. This holds when the source is partitioned by entity id.
- No ordering is assumed across entities.
- Final documents converge regardless of interleaving.
- **Known limitation:** if an entity's lookup key changes at the same moment its old dimension changes, the old `Lookup` instance may emit one stale enrichment after the new instance's output. The next change to that entity or its new dimension corrects it. Fixing this needs per-entity versioning, which is out of scope.
- **Known limitation:** with `requireMatch(true)`, an entity that moves from a key with a dimension to a key without one keeps its last enriched value downstream until that key gets a dimension or the entity changes again: the old key's relocation emits nothing (to avoid the cross-instance race) and the new key emits nothing (no match).

## Serialization

- `Change<T>` has an explicit `TypeInformation` and `TypeSerializer`:
  - the serializer writes one kind byte, the id, and the value using the element's own serializer;
  - it has a `TypeSerializerSnapshot` that stores the element serializer's snapshot, so savepoints restore across library versions.
- The internal streams get the same treatment: the tagged-union child stream of a `Nest` level, and the relocation-flagged entity stream of a `Lookup`.
- Nothing relies on Kryo. Tests run with generic types disabled (`pipeline.generic-types: false`).

## Build

- **Flink:** 2.2.1. `flink-streaming-java` and `flink-core` are `provided`. Java stays at 17.
- **Removed from the pom:**
  - `flink-connector-kafka`, `flink-connector-base`, `kafka-clients`;
  - `avro`, `flink-avro`, `flink-avro-confluent-registry`, `kafka-schema-registry-client`, and the Confluent repository;
  - the Testcontainers dependencies.
- **Test dependencies:** JUnit 5, AssertJ, and at 2.2.1 `flink-test-utils`, the `flink-streaming-java` test-jar, the `flink-runtime` test-jar, and the `flink-core` test-jar.
- **Unchanged:** the failsafe `integration-tests` profile, and the sources and javadoc jars.

## Removed code

Everything under `src/main/java/io/github/mannkostir/projections/` that exists today is deleted:

- `Director`, `StreamEnvironment`;
- `io/kafka/**`, `serializers/**`;
- `processing/**`, including `JobEndpoints`.

`Job`/`Director` scaffolding is not ported. It returns only if the spec 2 example shows real wiring boilerplate.

## Testing

Tests are written first, at four levels.

1. **Rules.** Each operator's decisions live in a plain package-private class with no Flink types in its logic: `NestRules`, `RoutingRules`, `LookupRules`. Every row of the Semantics tables is one JUnit test.
2. **Operator harness tests.** Flink keyed one-input and two-input harnesses cover:
   - every table row once through real state;
   - TTL and `orphanTimeout`, by advancing processing time;
   - a savepoint round-trip for `Nest`, `Lookup` and the routing step: snapshot mid-scenario, restore into a new harness, continue, and assert output equal to an uninterrupted run;
   - literal `uid` and state-descriptor names from the naming table.
3. **Serializer tests.** The `Change` serializer and the tagged-union serializer run through Flink's `SerializerTestBase`.
4. **Pipeline test.** `MiniClusterExtension` runs the resume model:
   - two `Nest` levels and one `Lookup`;
   - bounded in-memory sources, a collecting sink, and generic types disabled;
   - a scenario with a child before its parent, an experience moving between candidates, a company rename, a project delete and a candidate delete.

   It asserts the final converged document for each candidate, and that the deleted candidate ends in a `Delete`.

**Done:** `mvn -q verify` passes and a whole-branch review is clean.

## CLAUDE.md (local, gitignored)

- **Architecture:** replace the tree with the single-package layout and the Nest/Lookup/routing model.
- **Known debt:** remove the entries this spec resolves: singletons, `KafkaAdmin` side effects, the `OffsetStrategy` branching, `Director` wrapping, the `PrimitiveArraySerializer` ladder, Schema Registry not optional, no tests, and the README planned promises.
- **"Behaviour worth knowing":** replace the section with the naming contract, the routing step, and the ordering assumption.

## README

- Remove "(planned)" from the root-delete and per-stream TTL promises.
- Replace the dependencies sentence about Kafka, Avro and the Schema Registry client with: `Flink is \`provided\`: your job owns its version. Nothing is shaded or bundled.`

## Out of scope

- Kafka, Avro and Schema Registry adapters (spec 2).
- The Elasticsearch sink (spec 3).
- Release (spec 4).
- Per-record error policies.
- Event-time semantics.
- Deduplicating identical consecutive documents.
