# flink-projections: positioning and rename

## Goal

Reposition the library from "generic building blocks for common Flink use cases" to a solution for one use case, and give it a name that says so. A Flink engineer should be able to tell from the first paragraph of the README, within ten seconds, whether the library is for them.

## Positioning

**flink-projections** builds stateful Flink projections of entity streams into documents. Several change streams of related entities are joined by key and grouped into one document per root entity. That document stays correct as any part changes or is deleted, and it is emitted downstream as an upsert or a delete.

- **Flagship target:** a search index. The library was extracted from a resume-search ETL into Elasticsearch.
- **Other targets:** any sink, such as a Kafka topic, a cache or a document store.
- **Audience:** teams who have outgrown Flink SQL for this job because SQL owns the state. The library's value is control:
  - stable operator `uid`s and state names;
  - explicit delete semantics;
  - per-stream state TTL;
  - savepoint compatibility across releases.
- **Scope test for every future change:** does it serve "entity streams in, documents out, deletes honoured, state under the user's control"? Anything that doesn't is out of scope.

## Decisions

| Topic | Decision |
|---|---|
| Name | `flink-projections` |
| Maven coordinates | `io.github.mannkostir:flink-projections:0.1.0-SNAPSHOT` |
| Base package | `io.github.mannkostir.projections` (was `io.github.mannkostir.flink`) |
| Subpackage layout | Unchanged; restructuring belongs to the operator-first API spec |
| GitHub repository | Renamed to `mannkostir/flink-projections` by the owner; GitHub redirects the old URL |
| Rename scope | Everything now: artifactId, package, repository, README, CLAUDE.md |

## Deliverables

### 1. pom.xml

- `artifactId` and `name` are `flink-projections`.
- `description` is the README tagline.
- `url` and `scm` point at `github.com/mannkostir/flink-projections`.
- No dependency or plugin changes.

### 2. Package move

- Move every source file from `io.github.mannkostir.flink` to `io.github.mannkostir.projections`, keeping subpackage paths.
- Update every `package` and `import` line.
- Change no other lines.

### 3. README.md

The README has two parts:

- **Tagline:** "Stateful Flink projections of entity streams into documents: joins, grouping and deletes, with state you control."
- **Pitch**, in this order:
  1. The problem, told through the candidate / experiences / skills example.
  2. The pipeline: entity streams → keyed join → group into document → upsert or delete downstream.
  3. **Why not Flink SQL:** SQL owns the state. A query change or a Flink upgrade can re-plan the job and make savepoints unrestorable. Per-side TTL and delete semantics are out of reach. Debugging means reading generated code.
  4. **Promises:**
     - stable operator ids and state names as a compatibility contract;
     - explicit delete semantics, where a child removal updates the document and a root removal deletes it;
     - per-stream state lifecycle with a TTL the user sets;
     - convergence under replay, through at-least-once delivery and idempotent upserts keyed by document id;
     - plain operators usable in any job, with optional `Job` scaffolding.
  5. **Dependency note:** Flink, Kafka and Avro are `provided`.

Two promises are not yet implemented, and the README marks each with "(planned)":

- per-stream TTL;
- root removal deleting the document.

The README has no install or usage section until the operator-first API exists. It contains no code blocks.

### 4. CLAUDE.md (local, gitignored)

- **"What this repository is":** replace the section with the approved text. It covers the positioning above, the scope test, the operator-first shape, and the rule never to answer a design question with "use Flink SQL".
- **Architecture tree:** the root becomes `src/main/java/io/github/mannkostir/projections/`.
- **Known debt:** add an entry saying the README's per-stream TTL and root-delete promises are planned, not implemented.
- **MCP connections:** update the section to name the repository `mannkostir/flink-projections`.

CLAUDE.md is gitignored, so this change is local and not part of the commit.

### 5. Repository and remote

- **The owner** renames the repository with `gh repo rename flink-projections` or through the GitHub settings.
- **Then:**
  - `origin` becomes `https://github.com/mannkostir/flink-projections.git`;
  - the project's `.mcp.json` and CLAUDE.md name the new repository.

## Out of scope

- Operator-first public API, subpackage restructuring and Flink 2.x (the next spec).
- Implementing TTL or root-delete behaviour.
- Elasticsearch/OpenSearch sink module.
- Release pipeline and license choice.

## Compatibility

- **Savepoints:** the package move changes class names recorded in Kryo registrations, so savepoints taken before the move will not restore. No release exists, so no users are affected.
- **Operators and state:** `uid`s and state-descriptor names are unchanged, because they derive from literals and from user record classes.

## Verification

- `mvn -q clean verify` passes.
- `grep -r "io.github.mannkostir.flink\b"` finds no match in `src/` or `pom.xml`.
- The jar contains only `io/github/mannkostir/projections/**` and `META-INF`.
- `git remote -v` shows the renamed repository after the owner renames it.
