# flink-projections Positioning and Rename Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rename the library to `flink-projections` (coordinates, base package, repository) and give it a README pitch and a CLAUDE.md that state its single use case.

**Architecture:** The code does not change. This is a rename (pom identity, then a mechanical package move) plus documentation. Each task ends with a check that fails before the change and passes after it, followed by `mvn -q clean verify`.

**Tech Stack:** Maven, Java 17, git, `gh`.

**Spec:** `docs/superpowers/specs/2026-09-27-flink-projections-positioning-design.md`

## Global Constraints

- Maven coordinates are exactly `io.github.mannkostir:flink-projections:0.1.0-SNAPSHOT`.
- The base package is exactly `io.github.mannkostir.projections`, and subpackage paths stay unchanged below it.
- The repository URL is exactly `https://github.com/mannkostir/flink-projections`.
- The tagline is exactly: "Stateful Flink projections of entity streams into documents: joins, grouping and deletes, with state you control."
- Zero comments anywhere: no `//`, `/* */` or Javadoc, and no comments in README code blocks. The README has no code blocks at all.
- Commit messages are a single subject line: lowercase, imperative, 2 to 6 words, no trailing period, no body, no trailers.
- **Maven settings:** the user's `~/.m2/settings.xml` hides the Confluent repo.
  - Run every Maven command with `-s /private/tmp/claude-501/-Users-mannkostir-Documents-flink-use-cases-framework/3f51feaa-f321-4669-aad0-9b002a5b11e0/scratchpad/settings.xml`.
  - That file is the user's settings with `<mirrorOf>*,!confluent</mirrorOf>`. Recreate it if it is missing:
    ```
    sed 's#<mirrorOf>\*</mirrorOf>#<mirrorOf>*,!confluent</mirrorOf>#' ~/.m2/settings.xml > <that path>
    ```
- Do not rename the local checkout directory. Claude Code keys project memory on that path.
- Work on branch `flink-projections-positioning`, which already contains the spec commit.
- Never `git add -A`, `git add .` or `git commit -a`. `CLAUDE.md` and `.mcp.json` are gitignored; stage only the files each task names.

## Review Focus

1. **Stale compiled classes.** A build without `clean` can leave `io/github/mannkostir/flink/**` classes in `target/` and ship them in the jar. Every Maven check uses `clean verify`, and Task 2 asserts the jar contents.
2. **Over-eager rename.** A sed on `io.github.mannkostir.flink` must not touch `org.apache.flink` or the `flink` in `flink-projections`. Task 2 compares `org.apache.flink` counts before and after the move.
3. **Dead links before the repository rename.** Until the owner renames the repo, the pom `<url>`/`<scm>` point at a 404. Task 5 must run before the branch is merged, and its PR description says so.
4. **README over-claiming.** Per-stream TTL and root-delete are not implemented. Task 3's check asserts that both carry "(planned)".
5. **CLAUDE.md MCP section.** The section names `repo: flink-use-cases-framework`. If it is left stale, GitHub MCP calls hit a redirect or fail after the rename. Task 4 updates it, and Task 4's check greps for the old name.

---

### Task 1: pom identity

**Files:**
- Modify: `pom.xml:7`, `pom.xml:11-18`

**Interfaces:**
- Consumes: nothing.
- Produces: artifact `target/flink-projections-0.1.0-SNAPSHOT.jar`, plus its `-sources.jar` and `-javadoc.jar`.

- [ ] **Step 1: Write the failing check**

Run: `grep -c "flink-use-cases-framework" pom.xml`
Expected: `6`. The check passes when the count is `0`.

- [ ] **Step 2: Update identity elements**

Set these lines in `pom.xml` exactly:

```xml
  <artifactId>flink-projections</artifactId>
```

```xml
  <name>flink-projections</name>
  <description>Stateful Flink projections of entity streams into documents: joins, grouping and deletes, with state you control.</description>
  <url>https://github.com/mannkostir/flink-projections</url>

  <scm>
    <url>https://github.com/mannkostir/flink-projections</url>
    <connection>scm:git:https://github.com/mannkostir/flink-projections.git</connection>
    <developerConnection>scm:git:git@github.com:mannkostir/flink-projections.git</developerConnection>
  </scm>
```

Change nothing else in the pom.

- [ ] **Step 3: Run the check and the build**

Run: `grep -c "flink-use-cases-framework" pom.xml`
Expected: `0`

Run: `mvn -q -s <settings> clean verify && ls target/*.jar`
Expected: exit 0, and exactly these three files:
- `target/flink-projections-0.1.0-SNAPSHOT.jar`
- `target/flink-projections-0.1.0-SNAPSHOT-sources.jar`
- `target/flink-projections-0.1.0-SNAPSHOT-javadoc.jar`

- [ ] **Step 4: Commit**

```bash
git add pom.xml
git commit -m "rename artifact to flink-projections"
```

---

### Task 2: base package move

**Files:**
- Move: `src/main/java/io/github/mannkostir/flink/**` → `src/main/java/io/github/mannkostir/projections/**`
- Modify: the `package` and `import` lines of every moved file (38 files)

**Interfaces:**
- Consumes: Task 1's artifact name, used in the jar path.
- Produces: the base package `io.github.mannkostir.projections`. For example, `io.github.mannkostir.projections.processing.jobs.JobEndpoints` and `io.github.mannkostir.projections.Director`.

- [ ] **Step 1: Record the baseline and write the failing check**

Run: `grep -rho "org\.apache\.flink" src | wc -l`
Expected: `51`. Record this number.

Run: `grep -rn "io\.github\.mannkostir\.flink[.;]" src | wc -l`
Expected: a non-zero count (134 at planning time). The check passes when the count is `0`.

- [ ] **Step 2: Move the directory with history**

```bash
git mv src/main/java/io/github/mannkostir/flink src/main/java/io/github/mannkostir/projections
```

- [ ] **Step 3: Rewrite package and import lines**

```bash
grep -rl 'io\.github\.mannkostir\.flink[.;]' src | xargs sed -i '' 's/io\.github\.mannkostir\.flink\([.;]\)/io.github.mannkostir.projections\1/g'
```

- [ ] **Step 4: Run the checks**

Run: `grep -rn "io\.github\.mannkostir\.flink[.;]" src | wc -l`
Expected: `0`

Run: `grep -rho "org\.apache\.flink" src | wc -l`
Expected: the same number recorded in Step 1 (`51`).

Run: `git diff -M HEAD -- src | grep '^[-+]' | grep -v '^[-+][-+]' | grep -v '^[-+]\(package\|import\) '`
Expected: no output. Every changed line is a `package` or `import` line.

- [ ] **Step 5: Build and inspect the jar**

Run: `mvn -q -s <settings> clean verify`
Expected: exit 0.

Run: `unzip -l target/flink-projections-0.1.0-SNAPSHOT.jar | awk '{print $4}' | grep '\.class$' | grep -v '^io/github/mannkostir/projections/' | wc -l`
Expected: `0`

- [ ] **Step 6: Commit**

```bash
git add -u src
git add src/main/java/io/github/mannkostir/projections
git commit -m "move base package to projections"
```

---

### Task 3: README pitch

**Files:**
- Modify: `README.md` (currently empty)

**Interfaces:**
- Consumes: the tagline from Global Constraints.
- Produces: the public pitch, which later specs extend with install and usage sections.

- [ ] **Step 1: Write the failing check**

Run: `grep -c "(planned)" README.md; grep -c "Why not Flink SQL" README.md`
Expected: `0` and `0`. The check passes when the results are `2` and `1`.

- [ ] **Step 2: Write README.md**

Replace the whole file with exactly:

```markdown
# flink-projections

Stateful Flink projections of entity streams into documents: joins, grouping and deletes, with state you control.

You have entities arriving as change streams (candidates, their experiences, their skills) and you need one document per candidate in a search index, kept correct as any part changes or disappears. flink-projections is a set of DataStream operators for exactly that pipeline:

**entity streams → keyed join → group into document → upsert or delete downstream**

A search index is the flagship target, but a projection can feed any sink: a Kafka topic, a cache, a document store.

## Why not Flink SQL?

SQL can express the same joins, but it owns the state. A query change or a Flink upgrade can re-plan the job and leave your savepoint unrestorable, per-side TTL and delete semantics are out of reach, and debugging means reading generated code. flink-projections trades SQL's brevity for control:

- **Stable operator ids and state names**, treated as a compatibility contract across releases.
- **Explicit delete semantics**: removing a child entity updates the document; removing the root deletes it (planned).
- **Per-stream state lifecycle**, with each TTL set by you (planned).
- **Convergence under replay**: at-least-once delivery and idempotent upserts keyed by document id.
- **Plain operators** you wire into your own job, with optional `Job` scaffolding for the batteries-included path.

## Dependencies

Flink, Kafka and Avro are `provided`: your job owns their versions. Nothing is shaded or bundled.
```

- [ ] **Step 3: Run the check**

Run: `grep -c "(planned)" README.md; grep -c "Why not Flink SQL" README.md; grep -c '^```' README.md`
Expected: `2`, `1`, `0`

- [ ] **Step 4: Commit**

```bash
git add README.md
git commit -m "add readme pitch"
```

---

### Task 4: CLAUDE.md (local only, no commit)

**Files:**
- Modify: `CLAUDE.md`. It is gitignored, so this task makes no commit.

**Interfaces:**
- Consumes: the positioning text from the spec.
- Produces: the project guidance every future session loads.

- [ ] **Step 1: Write the failing check**

Run: `grep -c "flink-use-cases-framework\|mannkostir/flink/\|Composable pieces are the core" CLAUDE.md`
Expected: non-zero. The check passes when the count is `0`.

- [ ] **Step 2: Replace the opening of "What this repository is"**

- **Where:** the section runs from the heading `## What this repository is` down to the line `Consequences that shape every decision here:`. Keep that line and everything after it.
- **What to replace:** the section's first three blocks, which are
  1. the paragraph starting "An **open-source Java library**",
  2. the "Composable pieces" bullet,
  3. the "`Director` / `Job` is an optional thin layer" bullet.
- **Replace them with exactly:**

```markdown
**flink-projections** is an open-source Java library that builds stateful Flink projections of entity
streams into documents. Several change streams of related entities are joined by key and grouped into
one document per root entity. That document stays correct as any part changes or is deleted, and it is
emitted as an upsert or a delete downstream. A search index is the flagship target (the library was
extracted from a resume-search ETL into Elasticsearch), but a projection can feed any sink: a Kafka
topic, a cache, a document store.

The library exists for teams who have outgrown Flink SQL for this job and want **control**: stable
operator `uid`s and state names, explicit delete semantics, per-stream state TTL, and savepoint
compatibility across releases. Never answer a design question with "use Flink SQL"; SQL's hidden state
is the problem this library solves.

Scope test for every change: **does it serve "entity streams in, documents out, deletes honoured, state
under the user's control"?** If not, it doesn't belong here. Don't generalise it into a broader Flink
toolkit.

The shape is operator-first:

- **The operators are the product**: join, group-into-document and delete propagation, plus Kafka and
  Avro/Schema Registry serde at the edges. Each one is usable inside any hand-written Flink job.
- **`Director` / `Job` is an optional thin layer** for the batteries-included path. It composes the
  operators; the operators never depend on it.
```

- [ ] **Step 3: Update the architecture tree root**

Replace `src/main/java/io/github/mannkostir/flink/` with `src/main/java/io/github/mannkostir/projections/`.

- [ ] **Step 4: Add the known-debt entry**

Append this bullet as the last item of the `## Known debt` list:

```markdown
- **README promises not yet implemented**: per-stream state TTL and root-delete removing the document
  are marked "(planned)" in the README and are delivered by the operator-first API work.
```

- [ ] **Step 5: Update the MCP connections section**

Make two replacements:
- `github.com/mannkostir/flink-use-cases-framework` becomes `github.com/mannkostir/flink-projections`.
- `` `repo: flink-use-cases-framework` `` becomes `` `repo: flink-projections` ``.

- [ ] **Step 6: Run the check**

Run: `grep -c "flink-use-cases-framework\|mannkostir/flink/\|Composable pieces are the core" CLAUDE.md`
Expected: `0`

Run: `git status --short CLAUDE.md`
Expected: no output, because the file is gitignored.

---

### Task 5: repository rename and remote (owner action first)

**Files:**
- No tracked files. This task changes git config (`origin`) only. `.mcp.json` contains no repository name, so it needs no change.

**Interfaces:**
- Consumes: the owner has renamed the repository.
- Produces: `origin` points at `https://github.com/mannkostir/flink-projections.git`.

- [ ] **Step 1: Owner renames the repository**

The controller asks the user to run this; the controller does not run it:

```
! gh repo rename flink-projections --repo mannkostir/flink-use-cases-framework
```

`gh` must be signed in as `mannkostir`.

- [ ] **Step 2: Write the failing check**

Run: `git remote get-url origin`
Expected: `https://github.com/mannkostir/flink-use-cases-framework.git`. The check passes when it shows the new URL.

- [ ] **Step 3: Point origin at the new name**

```bash
git remote set-url origin https://github.com/mannkostir/flink-projections.git
```

- [ ] **Step 4: Verify the remote resolves**

Run: `git remote get-url origin && git ls-remote --heads origin main`
Expected: the new URL, then one line ending in `refs/heads/main`.

- [ ] **Step 5: Push and open the PR**

```bash
git push -u origin flink-projections-positioning
gh pr create --repo mannkostir/flink-projections --base main --head flink-projections-positioning --title "rename library to flink-projections" --body "$(cat <<'BODY'
## Summary
- Coordinates are now `io.github.mannkostir:flink-projections:0.1.0-SNAPSHOT`, and the base package is `io.github.mannkostir.projections`.
- The README pitch positions the library as stateful Flink projections of entity streams into documents.
- The spec and plan are under `docs/superpowers/`.

## Compatibility
The package move changes class names recorded in Kryo registrations, so savepoints taken before this change will not restore. `uid`s and state-descriptor names are unchanged. Nothing has been released yet.

## Test plan
- [x] `mvn -q clean verify` passes.
- [x] The jar contains only `io/github/mannkostir/projections/**` classes.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
BODY
)"
```

The owner merges the PR; the controller does not.
