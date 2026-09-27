# Operator-first API Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the `Job`-centred code with the operator-first `Change` / `Nest` / `Lookup` API on Flink 2.2.1. The API gives stable contract names, cascading deletes, re-parenting, per-stream TTL, and savepoint-tested state.

**Architecture:**
- Every public type and every package-private helper lives in one package, `io.github.mannkostir.projections`.
- Each operator splits into three parts:
  - a Flink-free rules class (`RoutingRules`, `NestRules`, `LookupRules`) that makes the decisions;
  - a small state interface with an in-memory test implementation and a Flink-state implementation;
  - a thin `KeyedProcessFunction` or `KeyedCoProcessFunction` adapter.
- Public builders (`Changes`, `Nest`, `Lookup`) wire the operators with explicit `uid`s, and every type has an explicit `TypeInformation` and serializer, so nothing falls back to Kryo.

**Tech Stack:** Java 17, Maven, Flink 2.2.1 (`provided`), JUnit 5, AssertJ, Flink test harnesses and `MiniClusterExtension`.

**Spec:** `docs/superpowers/specs/2026-09-28-operator-first-api-design.md`

## Global Constraints

- Flink `2.2.1`. `flink-core` and `flink-streaming-java` are `provided`. Java release `17`.
- Everything lives in the single package `io.github.mannkostir.projections`.
- Public types are exactly these:
  - `Change`, `Upsert`, `Delete`, `Changes`;
  - `Nest`, `ChildSlot`, `Children`, `Assembler`;
  - `Lookup` (including its nested `Lookup.WithDimension`), `Enricher`;
  - `NestOptions`, `ChildOptions`, `LookupOptions`, `ProjectionConfigurationException`.

  Serializer `Snapshot` classes are public nested classes only because Flink instantiates them reflectively. Everything else is package-private.
- Names and slot names match `[a-z][a-z0-9-]*`. Violations throw `ProjectionConfigurationException`.
- Operator `uid`s are `changes_<name>`, `nest_<name>`, `nest_<name>_route_<slot>`, `lookup_<name>` and `lookup_<name>_route`.
- State names are `<name>.parent`, `<name>.child.<slot>`, `<name>.last-doc`, `<name>.route.<slot>.last`, `<name>.entities`, `<name>.dimension` and `<name>.route.last`.
- Tests run with `PipelineOptions.GENERIC_TYPES` set to `false`.
- Zero comments in any file: no `//`, `/* */` or Javadoc. `@SuppressWarnings` annotations are allowed.
- Commit messages are one lowercase imperative subject of 2 to 6 words, with no body and no trailers.
- Every Maven command uses `-s /private/tmp/claude-501/-Users-mannkostir-Documents-flink-use-cases-framework/3f51feaa-f321-4669-aad0-9b002a5b11e0/scratchpad/settings.xml`. That file is the user's `~/.m2/settings.xml` with `<mirrorOf>*,!confluent</mirrorOf>`. If it's missing, recreate it with:
  ```bash
  sed 's#<mirrorOf>\*</mirrorOf>#<mirrorOf>*,!confluent</mirrorOf>#' ~/.m2/settings.xml > /private/tmp/claude-501/-Users-mannkostir-Documents-flink-use-cases-framework/3f51feaa-f321-4669-aad0-9b002a5b11e0/scratchpad/settings.xml
  ```
- Stage only the files each task names. Never `git add -A`, `git add .` or `git commit -a`. `CLAUDE.md` and `.mcp.json` are gitignored and never staged.
- Work on branch `operator-first-api`, which already contains the spec commits.
- **Verified code:** every code block in this plan was compiled and run against Flink 2.2.1: 123 tests, `mvn clean verify` green. Transcribe the blocks exactly. If a step's actual output differs from its Expected output, stop and report rather than improvising.

## Review Focus

1. **User code throws inside `Assembler` or `Enricher`.** The exception must propagate and fail the job, never be swallowed. Pinned by `NestRulesTest.assemblerFailurePropagates` (Task 7) and `LookupRulesTest.enricherFailurePropagates` (Task 11).
2. **A key selector returns `null`**, for example a child with a null foreign key. It must fail with a message naming the level or lookup, the slot, and the entity id, instead of a bare NullPointerException deep in Flink. Pinned by `KeySelectorsTest` (Task 12).
3. **Adding a new child slot to a level that already has a savepoint.** The savepoint must restore and the new slot must work, because slot state is stored per slot name. Pinned by `NestFunctionTest.restoresWhenSlotIsAdded` (Task 9).
4. **The assembler mutates the children list it receives.** The list must be unmodifiable and state must stay intact. Pinned by `NestRulesTest.childrenListIsUnmodifiable` (Task 7).
5. **A replayed upsert** (at-least-once redelivery) must produce an identical document. Pinned by `NestRulesTest.replayedUpsertEmitsIdenticalDocument` (Task 7).

---

### Task 1: Move the build to Flink 2.2.1 and remove the old code

**Files:**
- Modify: `pom.xml` (whole file)
- Delete: everything under `src/main/java/io/github/mannkostir/projections/`

**Interfaces:**
- Consumes: nothing.
- Produces: a build with no sources that compiles, tests and packages on Flink 2.2.1. Later tasks add sources to the same package directory.

- [ ] **Step 1: Write the failing check**

Run: `grep -c "flink.version>2.2.1" pom.xml; find src/main/java -name '*.java' | wc -l`
Expected: `0`, then a non-zero file count. The check passes when the results are `1` and `0`.

- [ ] **Step 2: Replace `pom.xml` with exactly:**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>

  <groupId>io.github.mannkostir</groupId>
  <artifactId>flink-projections</artifactId>
  <version>0.1.0-SNAPSHOT</version>
  <packaging>jar</packaging>

  <name>flink-projections</name>
  <description>Stateful Flink projections of entity streams into documents: joins, grouping and deletes, with state you control.</description>
  <url>https://github.com/mannkostir/flink-projections</url>

  <scm>
    <url>https://github.com/mannkostir/flink-projections</url>
    <connection>scm:git:https://github.com/mannkostir/flink-projections.git</connection>
    <developerConnection>scm:git:git@github.com:mannkostir/flink-projections.git</developerConnection>
  </scm>

  <properties>
    <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    <maven.compiler.release>17</maven.compiler.release>

    <flink.version>2.2.1</flink.version>

    <junit.version>5.14.4</junit.version>
    <assertj.version>3.27.7</assertj.version>

    <maven-compiler-plugin.version>3.14.1</maven-compiler-plugin.version>
    <maven-surefire-plugin.version>3.5.4</maven-surefire-plugin.version>
    <maven-failsafe-plugin.version>3.5.4</maven-failsafe-plugin.version>
    <maven-jar-plugin.version>3.4.2</maven-jar-plugin.version>
    <maven-source-plugin.version>3.3.1</maven-source-plugin.version>
    <maven-javadoc-plugin.version>3.11.3</maven-javadoc-plugin.version>
  </properties>

  <dependencyManagement>
    <dependencies>
      <dependency>
        <groupId>org.junit</groupId>
        <artifactId>junit-bom</artifactId>
        <version>${junit.version}</version>
        <type>pom</type>
        <scope>import</scope>
      </dependency>
    </dependencies>
  </dependencyManagement>

  <dependencies>
    <dependency>
      <groupId>org.apache.flink</groupId>
      <artifactId>flink-core</artifactId>
      <version>${flink.version}</version>
      <scope>provided</scope>
    </dependency>
    <dependency>
      <groupId>org.apache.flink</groupId>
      <artifactId>flink-streaming-java</artifactId>
      <version>${flink.version}</version>
      <scope>provided</scope>
    </dependency>

    <dependency>
      <groupId>org.junit.jupiter</groupId>
      <artifactId>junit-jupiter</artifactId>
      <scope>test</scope>
    </dependency>
    <dependency>
      <groupId>org.assertj</groupId>
      <artifactId>assertj-core</artifactId>
      <version>${assertj.version}</version>
      <scope>test</scope>
    </dependency>
    <dependency>
      <groupId>org.apache.flink</groupId>
      <artifactId>flink-test-utils</artifactId>
      <version>${flink.version}</version>
      <scope>test</scope>
    </dependency>
    <dependency>
      <groupId>org.apache.flink</groupId>
      <artifactId>flink-streaming-java</artifactId>
      <version>${flink.version}</version>
      <type>test-jar</type>
      <scope>test</scope>
    </dependency>
    <dependency>
      <groupId>org.apache.flink</groupId>
      <artifactId>flink-runtime</artifactId>
      <version>${flink.version}</version>
      <type>test-jar</type>
      <scope>test</scope>
    </dependency>
    <dependency>
      <groupId>org.apache.flink</groupId>
      <artifactId>flink-core</artifactId>
      <version>${flink.version}</version>
      <type>test-jar</type>
      <scope>test</scope>
    </dependency>
    <dependency>
      <groupId>org.apache.flink</groupId>
      <artifactId>flink-clients</artifactId>
      <version>${flink.version}</version>
      <scope>test</scope>
    </dependency>
  </dependencies>

  <build>
    <pluginManagement>
      <plugins>
        <plugin>
          <groupId>org.apache.maven.plugins</groupId>
          <artifactId>maven-compiler-plugin</artifactId>
          <version>${maven-compiler-plugin.version}</version>
        </plugin>
        <plugin>
          <groupId>org.apache.maven.plugins</groupId>
          <artifactId>maven-surefire-plugin</artifactId>
          <version>${maven-surefire-plugin.version}</version>
        </plugin>
        <plugin>
          <groupId>org.apache.maven.plugins</groupId>
          <artifactId>maven-failsafe-plugin</artifactId>
          <version>${maven-failsafe-plugin.version}</version>
        </plugin>
        <plugin>
          <groupId>org.apache.maven.plugins</groupId>
          <artifactId>maven-jar-plugin</artifactId>
          <version>${maven-jar-plugin.version}</version>
        </plugin>
      </plugins>
    </pluginManagement>

    <plugins>
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-source-plugin</artifactId>
        <version>${maven-source-plugin.version}</version>
        <executions>
          <execution>
            <id>attach-sources</id>
            <goals>
              <goal>jar-no-fork</goal>
            </goals>
          </execution>
        </executions>
      </plugin>
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-javadoc-plugin</artifactId>
        <version>${maven-javadoc-plugin.version}</version>
        <configuration>
          <doclint>none</doclint>
          <quiet>true</quiet>
        </configuration>
        <executions>
          <execution>
            <id>attach-javadocs</id>
            <goals>
              <goal>jar</goal>
            </goals>
          </execution>
        </executions>
      </plugin>
    </plugins>
  </build>

  <profiles>
    <profile>
      <id>integration-tests</id>
      <build>
        <plugins>
          <plugin>
            <groupId>org.apache.maven.plugins</groupId>
            <artifactId>maven-failsafe-plugin</artifactId>
            <executions>
              <execution>
                <goals>
                  <goal>integration-test</goal>
                  <goal>verify</goal>
                </goals>
              </execution>
            </executions>
          </plugin>
        </plugins>
      </build>
    </profile>
  </profiles>
</project>
```

- [ ] **Step 3: Delete the old sources**

```bash
git rm -r -q src/main/java/io/github/mannkostir/projections
```

- [ ] **Step 4: Run the check and the build**

Run: `grep -c "flink.version>2.2.1" pom.xml; find src/main/java -name '*.java' 2>/dev/null | wc -l`
Expected: `1`, then `0`.

Run: `mvn -q -s /private/tmp/claude-501/-Users-mannkostir-Documents-flink-use-cases-framework/3f51feaa-f321-4669-aad0-9b002a5b11e0/scratchpad/settings.xml clean verify && ls target/*.jar`
Expected: exit 0, and three jars: `flink-projections-0.1.0-SNAPSHOT.jar`, `-sources.jar` and `-javadoc.jar`.

- [ ] **Step 5: Commit**

```bash
git add pom.xml
git commit -m "move build to flink 2.2"
```

### Task 2: Change model

**Files:**
- Create: `src/main/java/io/github/mannkostir/projections/Change.java`
- Create: `src/main/java/io/github/mannkostir/projections/ChangeInvariants.java`
- Create: `src/main/java/io/github/mannkostir/projections/Upsert.java`
- Create: `src/main/java/io/github/mannkostir/projections/Delete.java`
- Test: `src/test/java/io/github/mannkostir/projections/ChangeTest.java`

**Interfaces:**
- Consumes: nothing
- Produces: public `sealed interface Change<T> permits Upsert, Delete` with `String id()`, `T value()`; public records `Upsert<T>(String id, T value)` and `Delete<T>(String id, T value)` that throw `IllegalArgumentException` on blank id or null value; package-private `ChangeInvariants.requireValid(String id, Object value)`.

- [ ] **Step 1: Write the failing test**

`src/test/java/io/github/mannkostir/projections/ChangeTest.java`:

```java
package io.github.mannkostir.projections;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class ChangeTest {
    @Test
    void upsertRejectsBlankId() {
        assertThatThrownBy(() -> new Upsert<>(" ", "value"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("id");
    }

    @Test
    void upsertRejectsNullValue() {
        assertThatThrownBy(() -> new Upsert<>("c1", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("value");
    }

    @Test
    void deleteRejectsNullId() {
        assertThatThrownBy(() -> new Delete<>(null, "value"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("id");
    }

    @Test
    void deleteRejectsNullValue() {
        assertThatThrownBy(() -> new Delete<>("c1", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("value");
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `mvn -q -s /private/tmp/claude-501/-Users-mannkostir-Documents-flink-use-cases-framework/3f51feaa-f321-4669-aad0-9b002a5b11e0/scratchpad/settings.xml test -Dtest=ChangeTest`
Expected: BUILD FAILURE with compilation errors: cannot find symbol `Upsert` / `Delete`.

- [ ] **Step 3: Write the implementation**

`src/main/java/io/github/mannkostir/projections/Change.java`:

```java
package io.github.mannkostir.projections;

public sealed interface Change<T> permits Upsert, Delete {
    String id();

    T value();
}
```

`src/main/java/io/github/mannkostir/projections/ChangeInvariants.java`:

```java
package io.github.mannkostir.projections;

final class ChangeInvariants {
    private ChangeInvariants() {
    }

    static void requireValid(String id, Object value) {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("Change id must be a non-blank string, got: " + id);
        }
        if (value == null) {
            throw new IllegalArgumentException("Change value must not be null for id " + id);
        }
    }
}
```

`src/main/java/io/github/mannkostir/projections/Upsert.java`:

```java
package io.github.mannkostir.projections;

public record Upsert<T>(String id, T value) implements Change<T> {
    public Upsert {
        ChangeInvariants.requireValid(id, value);
    }
}
```

`src/main/java/io/github/mannkostir/projections/Delete.java`:

```java
package io.github.mannkostir.projections;

public record Delete<T>(String id, T value) implements Change<T> {
    public Delete {
        ChangeInvariants.requireValid(id, value);
    }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `mvn -q -s /private/tmp/claude-501/-Users-mannkostir-Documents-flink-use-cases-framework/3f51feaa-f321-4669-aad0-9b002a5b11e0/scratchpad/settings.xml test -Dtest=ChangeTest`
Expected: exit 0, no test failures.

- [ ] **Step 5: Run the whole suite**

Run: `mvn -q -s /private/tmp/claude-501/-Users-mannkostir-Documents-flink-use-cases-framework/3f51feaa-f321-4669-aad0-9b002a5b11e0/scratchpad/settings.xml clean verify`
Expected: exit 0.

- [ ] **Step 6: Commit**

```bash
git add src/main/java/io/github/mannkostir/projections/Change.java src/main/java/io/github/mannkostir/projections/ChangeInvariants.java src/main/java/io/github/mannkostir/projections/Upsert.java src/main/java/io/github/mannkostir/projections/Delete.java src/test/java/io/github/mannkostir/projections/ChangeTest.java
git commit -m "add change model"
```

---

### Task 3: Change serialization

**Files:**
- Create: `src/main/java/io/github/mannkostir/projections/ChangeSerializer.java`
- Create: `src/main/java/io/github/mannkostir/projections/ChangeTypeInfo.java`
- Test: `src/test/java/io/github/mannkostir/projections/ChangeSerializerTest.java`

**Interfaces:**
- Consumes: `Change`, `Upsert`, `Delete` (Task 2).
- Produces: package-private `ChangeSerializer<T>(TypeSerializer<T> valueSerializer)` with public nested `ChangeSerializer.Snapshot<T>`; package-private `ChangeTypeInfo<T>(TypeInformation<T> valueType)`.

- [ ] **Step 1: Write the failing test**

`src/test/java/io/github/mannkostir/projections/ChangeSerializerTest.java`:

```java
package io.github.mannkostir.projections;

import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;

class ChangeSerializerTest extends SerializerTestBase<Change<String>> {
    @Override
    protected TypeSerializer<Change<String>> createSerializer() {
        return new ChangeSerializer<>(StringSerializer.INSTANCE);
    }

    @Override
    protected int getLength() {
        return -1;
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    protected Class<Change<String>> getTypeClass() {
        return (Class) Change.class;
    }

    @Override
    protected boolean allowNullInstances(TypeSerializer<Change<String>> serializer) {
        return true;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Change<String>[] getTestData() {
        return new Change[] {
                new Upsert<>("c1", "alice"),
                new Delete<>("c1", "alice"),
                new Upsert<>("c-2", ""),
                new Delete<>("c3", "日本")
        };
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `mvn -q -s /private/tmp/claude-501/-Users-mannkostir-Documents-flink-use-cases-framework/3f51feaa-f321-4669-aad0-9b002a5b11e0/scratchpad/settings.xml test -Dtest=ChangeSerializerTest`
Expected: BUILD FAILURE with compilation error: cannot find symbol `ChangeSerializer`.

- [ ] **Step 3: Write the implementation**

`src/main/java/io/github/mannkostir/projections/ChangeSerializer.java`:

```java
package io.github.mannkostir.projections;

import java.io.IOException;

import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.types.StringValue;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

final class ChangeSerializer<T> extends TypeSerializer<Change<T>> {
    private static final byte UPSERT = 0;
    private static final byte DELETE = 1;

    private final TypeSerializer<T> valueSerializer;

    ChangeSerializer(TypeSerializer<T> valueSerializer) {
        this.valueSerializer = valueSerializer;
    }

    TypeSerializer<T> valueSerializer() {
        return valueSerializer;
    }

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TypeSerializer<Change<T>> duplicate() {
        TypeSerializer<T> duplicated = valueSerializer.duplicate();
        return duplicated == valueSerializer ? this : new ChangeSerializer<>(duplicated);
    }

    @Override
    public Change<T> createInstance() {
        return null;
    }

    @Override
    public Change<T> copy(Change<T> from) {
        T value = valueSerializer.copy(from.value());
        return from instanceof Delete ? new Delete<>(from.id(), value) : new Upsert<>(from.id(), value);
    }

    @Override
    public Change<T> copy(Change<T> from, Change<T> reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(Change<T> change, DataOutputView target) throws IOException {
        target.writeByte(change instanceof Delete ? DELETE : UPSERT);
        StringValue.writeString(change.id(), target);
        valueSerializer.serialize(change.value(), target);
    }

    @Override
    public Change<T> deserialize(DataInputView source) throws IOException {
        byte kind = source.readByte();
        String id = StringValue.readString(source);
        T value = valueSerializer.deserialize(source);
        return kind == DELETE ? new Delete<>(id, value) : new Upsert<>(id, value);
    }

    @Override
    public Change<T> deserialize(Change<T> reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        target.writeByte(source.readByte());
        StringValue.copyString(source, target);
        valueSerializer.copy(source, target);
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof ChangeSerializer<?> that && valueSerializer.equals(that.valueSerializer);
    }

    @Override
    public int hashCode() {
        return valueSerializer.hashCode();
    }

    @Override
    public TypeSerializerSnapshot<Change<T>> snapshotConfiguration() {
        return new Snapshot<>(this);
    }

    public static final class Snapshot<T> extends CompositeTypeSerializerSnapshot<Change<T>, ChangeSerializer<T>> {
        private static final int VERSION = 1;

        public Snapshot() {
        }

        Snapshot(ChangeSerializer<T> serializer) {
            super(serializer);
        }

        @Override
        protected int getCurrentOuterSnapshotVersion() {
            return VERSION;
        }

        @Override
        protected TypeSerializer<?>[] getNestedSerializers(ChangeSerializer<T> outerSerializer) {
            return new TypeSerializer<?>[] {outerSerializer.valueSerializer};
        }

        @Override
        @SuppressWarnings("unchecked")
        protected ChangeSerializer<T> createOuterSerializerWithNestedSerializers(TypeSerializer<?>[] nestedSerializers) {
            return new ChangeSerializer<>((TypeSerializer<T>) nestedSerializers[0]);
        }
    }
}
```

`src/main/java/io/github/mannkostir/projections/ChangeTypeInfo.java`:

```java
package io.github.mannkostir.projections;

import java.util.Objects;

import org.apache.flink.api.common.serialization.SerializerConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

final class ChangeTypeInfo<T> extends TypeInformation<Change<T>> {
    private final TypeInformation<T> valueType;

    ChangeTypeInfo(TypeInformation<T> valueType) {
        this.valueType = Objects.requireNonNull(valueType, "valueType");
    }

    TypeInformation<T> valueType() {
        return valueType;
    }

    @Override
    public boolean isBasicType() {
        return false;
    }

    @Override
    public boolean isTupleType() {
        return false;
    }

    @Override
    public int getArity() {
        return 1;
    }

    @Override
    public int getTotalFields() {
        return 1;
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public Class<Change<T>> getTypeClass() {
        return (Class) Change.class;
    }

    @Override
    public boolean isKeyType() {
        return false;
    }

    @Override
    public TypeSerializer<Change<T>> createSerializer(SerializerConfig config) {
        return new ChangeSerializer<>(valueType.createSerializer(config));
    }

    @Override
    public String toString() {
        return "Change<" + valueType + ">";
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof ChangeTypeInfo<?> that && that.canEqual(this) && valueType.equals(that.valueType);
    }

    @Override
    public int hashCode() {
        return valueType.hashCode();
    }

    @Override
    public boolean canEqual(Object other) {
        return other instanceof ChangeTypeInfo;
    }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `mvn -q -s /private/tmp/claude-501/-Users-mannkostir-Documents-flink-use-cases-framework/3f51feaa-f321-4669-aad0-9b002a5b11e0/scratchpad/settings.xml test -Dtest=ChangeSerializerTest`
Expected: exit 0, no test failures.

- [ ] **Step 5: Run the whole suite**

Run: `mvn -q -s /private/tmp/claude-501/-Users-mannkostir-Documents-flink-use-cases-framework/3f51feaa-f321-4669-aad0-9b002a5b11e0/scratchpad/settings.xml clean verify`
Expected: exit 0.

- [ ] **Step 6: Commit**

```bash
git add src/main/java/io/github/mannkostir/projections/ChangeSerializer.java src/main/java/io/github/mannkostir/projections/ChangeTypeInfo.java src/test/java/io/github/mannkostir/projections/ChangeSerializerTest.java
git commit -m "add change serializer"
```

---

### Task 4: Changes adapter and naming contract

**Files:**
- Create: `src/main/java/io/github/mannkostir/projections/ProjectionConfigurationException.java`
- Create: `src/main/java/io/github/mannkostir/projections/Names.java`
- Create: `src/main/java/io/github/mannkostir/projections/ContractNames.java`
- Create: `src/main/java/io/github/mannkostir/projections/ToChange.java`
- Create: `src/main/java/io/github/mannkostir/projections/Changes.java`
- Test: `src/test/java/io/github/mannkostir/projections/ContractNamesTest.java`
- Test: `src/test/java/io/github/mannkostir/projections/ChangesTest.java`

**Interfaces:**
- Consumes: `Change`, `Upsert`, `Delete` (Task 2); `ChangeTypeInfo` (Task 3).
- Produces: public `Changes.typeInfo(TypeInformation<T>)` and `Changes.from(String name, DataStream<T>, KeySelector<T,String> id, FilterFunction<T> isDeleted, TypeInformation<T>)`; public `ProjectionConfigurationException`; package-private `Names.requireValid(String name, String role)` and every `ContractNames` method (`changesUid`, `nestUid`, `nestRouteUid`, `lookupUid`, `lookupRouteUid`, `nestParentState`, `nestChildState`, `nestLastDocState`, `nestRouteState`, `lookupEntitiesState`, `lookupDimensionState`, `lookupRouteState`); test helper `ChangesTest.environment()` returning a `StreamExecutionEnvironment` with generic types disabled, reused by later tests.

- [ ] **Step 1: Write the failing tests**

`src/test/java/io/github/mannkostir/projections/ContractNamesTest.java`:

```java
package io.github.mannkostir.projections;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class ContractNamesTest {
    @Test
    void operatorUids() {
        assertThat(ContractNames.changesUid("rows")).isEqualTo("changes_rows");
        assertThat(ContractNames.nestUid("candidate")).isEqualTo("nest_candidate");
        assertThat(ContractNames.nestRouteUid("candidate", "experiences")).isEqualTo("nest_candidate_route_experiences");
        assertThat(ContractNames.lookupUid("company")).isEqualTo("lookup_company");
        assertThat(ContractNames.lookupRouteUid("company")).isEqualTo("lookup_company_route");
    }

    @Test
    void stateDescriptorNames() {
        assertThat(ContractNames.nestParentState("candidate")).isEqualTo("candidate.parent");
        assertThat(ContractNames.nestChildState("candidate", "experiences")).isEqualTo("candidate.child.experiences");
        assertThat(ContractNames.nestLastDocState("candidate")).isEqualTo("candidate.last-doc");
        assertThat(ContractNames.nestRouteState("candidate", "experiences")).isEqualTo("candidate.route.experiences.last");
        assertThat(ContractNames.lookupEntitiesState("company")).isEqualTo("company.entities");
        assertThat(ContractNames.lookupDimensionState("company")).isEqualTo("company.dimension");
        assertThat(ContractNames.lookupRouteState("company")).isEqualTo("company.route.last");
    }
}
```

`src/test/java/io/github/mannkostir/projections/ChangesTest.java`:

```java
package io.github.mannkostir.projections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class ChangesTest {
    @RegisterExtension
    static final MiniClusterExtension CLUSTER = new MiniClusterExtension();

    @Test
    void mapsDeletedRowsToDeleteAndOthersToUpsert() throws Exception {
        StreamExecutionEnvironment env = environment();
        DataStream<String> rows = env.fromData(Types.STRING, "a:live", "b:gone");

        DataStream<Change<String>> changes = Changes.from(
                "rows", rows, row -> row.split(":")[0], row -> row.endsWith("gone"), Types.STRING);

        assertThat(changes.executeAndCollect(10)).containsExactlyInAnyOrder(
                new Upsert<>("a", "a:live"),
                new Delete<>("b", "b:gone"));
    }

    @Test
    void rejectsInvalidName() {
        StreamExecutionEnvironment env = environment();
        DataStream<String> rows = env.fromData(Types.STRING, "a:live");

        assertThatThrownBy(() -> Changes.from("Rows", rows, row -> row, row -> false, Types.STRING))
                .isInstanceOf(ProjectionConfigurationException.class)
                .hasMessageContaining("'Rows'");
    }

    @Test
    void usesNameInUid() {
        StreamExecutionEnvironment env = environment();
        DataStream<String> rows = env.fromData(Types.STRING, "a:live");

        DataStream<Change<String>> changes = Changes.from("rows", rows, row -> row, row -> false, Types.STRING);

        assertThat(changes.getTransformation().getUid()).isEqualTo("changes_rows");
    }

    static StreamExecutionEnvironment environment() {
        Configuration configuration = new Configuration();
        configuration.set(PipelineOptions.GENERIC_TYPES, false);
        return StreamExecutionEnvironment.getExecutionEnvironment(configuration);
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `mvn -q -s /private/tmp/claude-501/-Users-mannkostir-Documents-flink-use-cases-framework/3f51feaa-f321-4669-aad0-9b002a5b11e0/scratchpad/settings.xml test -Dtest=ContractNamesTest,ChangesTest`
Expected: BUILD FAILURE with compilation errors: cannot find symbol `ContractNames` / `Changes` / `ProjectionConfigurationException`.

- [ ] **Step 3: Write the implementation**

`src/main/java/io/github/mannkostir/projections/ProjectionConfigurationException.java`:

```java
package io.github.mannkostir.projections;

public final class ProjectionConfigurationException extends RuntimeException {
    ProjectionConfigurationException(String message) {
        super(message);
    }
}
```

`src/main/java/io/github/mannkostir/projections/Names.java`:

```java
package io.github.mannkostir.projections;

import java.util.regex.Pattern;

final class Names {
    private static final Pattern VALID = Pattern.compile("[a-z][a-z0-9-]*");

    private Names() {
    }

    static String requireValid(String name, String role) {
        if (name == null || !VALID.matcher(name).matches()) {
            throw new ProjectionConfigurationException(
                    role + " '" + name + "' is invalid: use lowercase letters, digits and '-', starting with a letter, e.g. 'candidate'");
        }
        return name;
    }
}
```

`src/main/java/io/github/mannkostir/projections/ContractNames.java`:

```java
package io.github.mannkostir.projections;

final class ContractNames {
    private ContractNames() {
    }

    static String changesUid(String name) {
        return "changes_" + name;
    }

    static String nestUid(String name) {
        return "nest_" + name;
    }

    static String nestRouteUid(String name, String slot) {
        return "nest_" + name + "_route_" + slot;
    }

    static String lookupUid(String name) {
        return "lookup_" + name;
    }

    static String lookupRouteUid(String name) {
        return "lookup_" + name + "_route";
    }

    static String nestParentState(String name) {
        return name + ".parent";
    }

    static String nestChildState(String name, String slot) {
        return name + ".child." + slot;
    }

    static String nestLastDocState(String name) {
        return name + ".last-doc";
    }

    static String nestRouteState(String name, String slot) {
        return name + ".route." + slot + ".last";
    }

    static String lookupEntitiesState(String name) {
        return name + ".entities";
    }

    static String lookupDimensionState(String name) {
        return name + ".dimension";
    }

    static String lookupRouteState(String name) {
        return name + ".route.last";
    }
}
```

`src/main/java/io/github/mannkostir/projections/ToChange.java`:

```java
package io.github.mannkostir.projections;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;

final class ToChange<T> implements MapFunction<T, Change<T>> {
    private final KeySelector<T, String> id;
    private final FilterFunction<T> isDeleted;

    ToChange(KeySelector<T, String> id, FilterFunction<T> isDeleted) {
        this.id = id;
        this.isDeleted = isDeleted;
    }

    @Override
    public Change<T> map(T value) throws Exception {
        String changeId = id.getKey(value);
        return isDeleted.filter(value) ? new Delete<>(changeId, value) : new Upsert<>(changeId, value);
    }
}
```

`src/main/java/io/github/mannkostir/projections/Changes.java`:

```java
package io.github.mannkostir.projections;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;

public final class Changes {
    private Changes() {
    }

    public static <T> TypeInformation<Change<T>> typeInfo(TypeInformation<T> type) {
        return new ChangeTypeInfo<>(type);
    }

    public static <T> DataStream<Change<T>> from(
            String name,
            DataStream<T> stream,
            KeySelector<T, String> id,
            FilterFunction<T> isDeleted,
            TypeInformation<T> type) {
        String uid = ContractNames.changesUid(Names.requireValid(name, "Changes name"));
        return stream.map(new ToChange<>(id, isDeleted), typeInfo(type)).uid(uid).name(uid);
    }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `mvn -q -s /private/tmp/claude-501/-Users-mannkostir-Documents-flink-use-cases-framework/3f51feaa-f321-4669-aad0-9b002a5b11e0/scratchpad/settings.xml test -Dtest=ContractNamesTest,ChangesTest`
Expected: exit 0, no test failures.

- [ ] **Step 5: Run the whole suite**

Run: `mvn -q -s /private/tmp/claude-501/-Users-mannkostir-Documents-flink-use-cases-framework/3f51feaa-f321-4669-aad0-9b002a5b11e0/scratchpad/settings.xml clean verify`
Expected: exit 0.

- [ ] **Step 6: Commit**

```bash
git add src/main/java/io/github/mannkostir/projections/ProjectionConfigurationException.java src/main/java/io/github/mannkostir/projections/Names.java src/main/java/io/github/mannkostir/projections/ContractNames.java src/main/java/io/github/mannkostir/projections/ToChange.java src/main/java/io/github/mannkostir/projections/Changes.java src/test/java/io/github/mannkostir/projections/ContractNamesTest.java src/test/java/io/github/mannkostir/projections/ChangesTest.java
git commit -m "add changes adapter"
```

---

### Task 5: Routing step

**Files:**
- Create: `src/main/java/io/github/mannkostir/projections/Routed.java`
- Create: `src/main/java/io/github/mannkostir/projections/RoutingRules.java`
- Create: `src/main/java/io/github/mannkostir/projections/ChangeTagger.java`
- Create: `src/main/java/io/github/mannkostir/projections/StateTtl.java`
- Create: `src/main/java/io/github/mannkostir/projections/RoutingFunction.java`
- Test: `src/test/java/io/github/mannkostir/projections/RoutingRulesTest.java`
- Test: `src/test/java/io/github/mannkostir/projections/RoutingFunctionTest.java`

**Interfaces:**
- Consumes: `Change`, `Upsert`, `Delete` (Task 2); `ChangeSerializer` (Task 3).
- Produces: package-private `record Routed<T>(Change<T> change, boolean relocation)`; `RoutingRules<T>(KeySelector<T,String> targetKey)` with `List<Routed<T>> route(T lastValue, Change<T> incoming)`; `interface ChangeTagger<T,R> extends Serializable { R tag(Routed<T> routed); }`; `StateTtl.applyTo(D descriptor, Optional<Duration> ttl)`; `RoutingFunction<T,R>(String stateName, KeySelector<T,String> targetKey, TypeInformation<T> valueType, Optional<Duration> stateTtl, ChangeTagger<T,R> tagger)`, a `KeyedProcessFunction<String, Change<T>, R>`.

- [ ] **Step 1: Write the failing tests**

`src/test/java/io/github/mannkostir/projections/RoutingRulesTest.java`:

```java
package io.github.mannkostir.projections;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class RoutingRulesTest {
    private final RoutingRules<String> rules = new RoutingRules<>(value -> value.split("@")[1]);

    @Test
    void firstUpsertIsForwarded() throws Exception {
        assertThat(rules.route(null, new Upsert<>("e1", "e1@a")))
                .containsExactly(new Routed<>(new Upsert<>("e1", "e1@a"), false));
    }

    @Test
    void upsertToSameTargetIsForwarded() throws Exception {
        assertThat(rules.route("e1@a", new Upsert<>("e1", "e1-renamed@a")))
                .containsExactly(new Routed<>(new Upsert<>("e1", "e1-renamed@a"), false));
    }

    @Test
    void upsertToNewTargetRelocatesFromOldTargetFirst() throws Exception {
        assertThat(rules.route("e1@a", new Upsert<>("e1", "e1@b"))).containsExactly(
                new Routed<>(new Delete<>("e1", "e1@a"), true),
                new Routed<>(new Upsert<>("e1", "e1@b"), false));
    }

    @Test
    void deleteIsForwarded() throws Exception {
        assertThat(rules.route("e1@a", new Delete<>("e1", "e1@a")))
                .containsExactly(new Routed<>(new Delete<>("e1", "e1@a"), false));
    }
}
```

`src/test/java/io/github/mannkostir/projections/RoutingFunctionTest.java`:

```java
package io.github.mannkostir.projections;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Optional;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

class RoutingFunctionTest {
    private static final String STATE = "candidate.route.experiences.last";

    @Test
    void emitsDeleteForOldTargetWhenTargetChanges() throws Exception {
        try (var harness = harness(Optional.empty())) {
            harness.open();
            harness.processElement(new Upsert<>("e1", "e1@a"), 1L);
            harness.processElement(new Upsert<>("e1", "e1@b"), 2L);

            assertThat(harness.extractOutputValues()).containsExactly(
                    new Upsert<>("e1", "e1@a"),
                    new Delete<>("e1", "e1@a"),
                    new Upsert<>("e1", "e1@b"));
        }
    }

    @Test
    void remembersLastValueAcrossSavepoint() throws Exception {
        OperatorSubtaskState snapshot;
        try (var harness = harness(Optional.empty())) {
            harness.open();
            harness.processElement(new Upsert<>("e1", "e1@a"), 1L);
            snapshot = harness.snapshot(1L, 1L);
        }

        try (var restored = harness(Optional.empty())) {
            restored.initializeState(snapshot);
            restored.open();
            restored.processElement(new Upsert<>("e1", "e1@b"), 2L);

            assertThat(restored.extractOutputValues()).containsExactly(
                    new Delete<>("e1", "e1@a"),
                    new Upsert<>("e1", "e1@b"));
        }
    }

    @Test
    void storesLastValueUnderGivenStateName() throws Exception {
        try (var harness = harness(Optional.empty())) {
            harness.open();
            harness.processElement(new Upsert<>("e1", "e1@a"), 1L);

            assertThat(harness.getOperator().getKeyedStateBackend().getKeys(STATE, VoidNamespace.INSTANCE))
                    .containsExactly("e1");
        }
    }

    @Test
    void forgetsLastValueAfterTtl() throws Exception {
        try (var harness = harness(Optional.of(Duration.ofMillis(100)))) {
            harness.open();
            harness.setStateTtlProcessingTime(0L);
            harness.processElement(new Upsert<>("e1", "e1@a"), 1L);
            harness.setStateTtlProcessingTime(200L);
            harness.processElement(new Upsert<>("e1", "e1@b"), 2L);

            assertThat(harness.extractOutputValues()).containsExactly(
                    new Upsert<>("e1", "e1@a"),
                    new Upsert<>("e1", "e1@b"));
        }
    }

    private static KeyedOneInputStreamOperatorTestHarness<String, Change<String>, Change<String>> harness(
            Optional<Duration> ttl) throws Exception {
        RoutingFunction<String, Change<String>> function = new RoutingFunction<>(
                STATE, value -> value.split("@")[1], Types.STRING, ttl, Routed::change);
        var harness = new KeyedOneInputStreamOperatorTestHarness<String, Change<String>, Change<String>>(
                new KeyedProcessOperator<>(function), Change::id, Types.STRING);
        harness.setup(new ChangeSerializer<>(StringSerializer.INSTANCE));
        return harness;
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `mvn -q -s /private/tmp/claude-501/-Users-mannkostir-Documents-flink-use-cases-framework/3f51feaa-f321-4669-aad0-9b002a5b11e0/scratchpad/settings.xml test -Dtest=RoutingRulesTest,RoutingFunctionTest`
Expected: BUILD FAILURE with compilation errors: cannot find symbol `RoutingRules` / `Routed` / `RoutingFunction`.

- [ ] **Step 3: Write the implementation**

`src/main/java/io/github/mannkostir/projections/Routed.java`:

```java
package io.github.mannkostir.projections;

record Routed<T>(Change<T> change, boolean relocation) {
}
```

`src/main/java/io/github/mannkostir/projections/RoutingRules.java`:

```java
package io.github.mannkostir.projections;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

import org.apache.flink.api.java.functions.KeySelector;

final class RoutingRules<T> implements Serializable {
    private final KeySelector<T, String> targetKey;

    RoutingRules(KeySelector<T, String> targetKey) {
        this.targetKey = targetKey;
    }

    List<Routed<T>> route(T lastValue, Change<T> incoming) throws Exception {
        if (incoming instanceof Upsert<T> && lastValue != null && movesTarget(lastValue, incoming.value())) {
            return List.of(new Routed<>(new Delete<>(incoming.id(), lastValue), true), new Routed<>(incoming, false));
        }
        return List.of(new Routed<>(incoming, false));
    }

    private boolean movesTarget(T lastValue, T newValue) throws Exception {
        return !Objects.equals(targetKey.getKey(lastValue), targetKey.getKey(newValue));
    }
}
```

`src/main/java/io/github/mannkostir/projections/ChangeTagger.java`:

```java
package io.github.mannkostir.projections;

import java.io.Serializable;

interface ChangeTagger<T, R> extends Serializable {
    R tag(Routed<T> routed);
}
```

`src/main/java/io/github/mannkostir/projections/StateTtl.java`:

```java
package io.github.mannkostir.projections;

import java.time.Duration;
import java.util.Optional;

import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;

final class StateTtl {
    private StateTtl() {
    }

    static <D extends StateDescriptor<?, ?>> D applyTo(D descriptor, Optional<Duration> ttl) {
        ttl.map(StateTtl::config).ifPresent(descriptor::enableTimeToLive);
        return descriptor;
    }

    private static StateTtlConfig config(Duration ttl) {
        return StateTtlConfig.newBuilder(ttl)
                .updateTtlOnCreateAndWrite()
                .neverReturnExpired()
                .build();
    }
}
```

`src/main/java/io/github/mannkostir/projections/RoutingFunction.java`:

```java
package io.github.mannkostir.projections;

import java.time.Duration;
import java.util.Optional;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

final class RoutingFunction<T, R> extends KeyedProcessFunction<String, Change<T>, R> {
    private final String stateName;
    private final TypeInformation<T> valueType;
    private final Duration stateTtl;
    private final RoutingRules<T> rules;
    private final ChangeTagger<T, R> tagger;
    private transient ValueState<T> lastValue;

    RoutingFunction(
            String stateName,
            KeySelector<T, String> targetKey,
            TypeInformation<T> valueType,
            Optional<Duration> stateTtl,
            ChangeTagger<T, R> tagger) {
        this.stateName = stateName;
        this.valueType = valueType;
        this.stateTtl = stateTtl.orElse(null);
        this.rules = new RoutingRules<>(targetKey);
        this.tagger = tagger;
    }

    @Override
    public void open(OpenContext openContext) {
        ValueStateDescriptor<T> descriptor = new ValueStateDescriptor<>(stateName, valueType);
        lastValue = getRuntimeContext().getState(StateTtl.applyTo(descriptor, Optional.ofNullable(stateTtl)));
    }

    @Override
    public void processElement(Change<T> change, Context context, Collector<R> out) throws Exception {
        for (Routed<T> routed : rules.route(lastValue.value(), change)) {
            out.collect(tagger.tag(routed));
        }
        remember(change);
    }

    private void remember(Change<T> change) throws Exception {
        if (change instanceof Delete<T>) {
            lastValue.clear();
        } else {
            lastValue.update(change.value());
        }
    }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `mvn -q -s /private/tmp/claude-501/-Users-mannkostir-Documents-flink-use-cases-framework/3f51feaa-f321-4669-aad0-9b002a5b11e0/scratchpad/settings.xml test -Dtest=RoutingRulesTest,RoutingFunctionTest`
Expected: exit 0, no test failures.

- [ ] **Step 5: Run the whole suite**

Run: `mvn -q -s /private/tmp/claude-501/-Users-mannkostir-Documents-flink-use-cases-framework/3f51feaa-f321-4669-aad0-9b002a5b11e0/scratchpad/settings.xml clean verify`
Expected: exit 0.

- [ ] **Step 6: Commit**

```bash
git add src/main/java/io/github/mannkostir/projections/Routed.java src/main/java/io/github/mannkostir/projections/RoutingRules.java src/main/java/io/github/mannkostir/projections/ChangeTagger.java src/main/java/io/github/mannkostir/projections/StateTtl.java src/main/java/io/github/mannkostir/projections/RoutingFunction.java src/test/java/io/github/mannkostir/projections/RoutingRulesTest.java src/test/java/io/github/mannkostir/projections/RoutingFunctionTest.java
git commit -m "add routing step"
```

---

### Task 6: Options

**Files:**
- Create: `src/main/java/io/github/mannkostir/projections/Durations.java`
- Create: `src/main/java/io/github/mannkostir/projections/NestOptions.java`
- Create: `src/main/java/io/github/mannkostir/projections/ChildOptions.java`
- Create: `src/main/java/io/github/mannkostir/projections/LookupOptions.java`
- Test: `src/test/java/io/github/mannkostir/projections/OptionsTest.java`

**Interfaces:**
- Consumes: `ProjectionConfigurationException` (Task 4).
- Produces: public `NestOptions` (`defaults()`, `builder().parentStateTtl(Duration).orphanTimeout(Duration).build()`, package-private `Optional<Duration> parentStateTtl()`, `orphanTimeout()`); public `ChildOptions` (`defaults()`, `builder().stateTtl(Duration).build()`, package-private `Optional<Duration> stateTtl()`); public `LookupOptions` (`defaults()`, `builder().stateTtl(Duration).requireMatch(boolean).build()`, package-private `stateTtl()`, `boolean requireMatch()`).

- [ ] **Step 1: Write the failing test**

`src/test/java/io/github/mannkostir/projections/OptionsTest.java`:

```java
package io.github.mannkostir.projections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;

import org.junit.jupiter.api.Test;

class OptionsTest {
    @Test
    void defaultsHaveNoTtlNoOrphanTimeoutAndLeftJoin() {
        assertThat(NestOptions.defaults().parentStateTtl()).isEmpty();
        assertThat(NestOptions.defaults().orphanTimeout()).isEmpty();
        assertThat(ChildOptions.defaults().stateTtl()).isEmpty();
        assertThat(LookupOptions.defaults().stateTtl()).isEmpty();
        assertThat(LookupOptions.defaults().requireMatch()).isFalse();
    }

    @Test
    void rejectsZeroParentStateTtl() {
        assertThatThrownBy(() -> NestOptions.builder().parentStateTtl(Duration.ZERO))
                .isInstanceOf(ProjectionConfigurationException.class)
                .hasMessageContaining("parentStateTtl");
    }

    @Test
    void rejectsNegativeOrphanTimeout() {
        assertThatThrownBy(() -> NestOptions.builder().orphanTimeout(Duration.ofSeconds(-1)))
                .isInstanceOf(ProjectionConfigurationException.class)
                .hasMessageContaining("orphanTimeout");
    }

    @Test
    void rejectsNullChildStateTtl() {
        assertThatThrownBy(() -> ChildOptions.builder().stateTtl(null))
                .isInstanceOf(ProjectionConfigurationException.class)
                .hasMessageContaining("stateTtl");
    }

    @Test
    void rejectsZeroLookupStateTtl() {
        assertThatThrownBy(() -> LookupOptions.builder().stateTtl(Duration.ZERO))
                .isInstanceOf(ProjectionConfigurationException.class)
                .hasMessageContaining("stateTtl");
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `mvn -q -s /private/tmp/claude-501/-Users-mannkostir-Documents-flink-use-cases-framework/3f51feaa-f321-4669-aad0-9b002a5b11e0/scratchpad/settings.xml test -Dtest=OptionsTest`
Expected: BUILD FAILURE with compilation errors: cannot find symbol `NestOptions` / `ChildOptions` / `LookupOptions`.

- [ ] **Step 3: Write the implementation**

`src/main/java/io/github/mannkostir/projections/Durations.java`:

```java
package io.github.mannkostir.projections;

import java.time.Duration;

final class Durations {
    private Durations() {
    }

    static Duration requirePositive(Duration duration, String option) {
        if (duration == null || duration.isZero() || duration.isNegative()) {
            throw new ProjectionConfigurationException(option + " must be a positive duration, got: " + duration);
        }
        return duration;
    }
}
```

`src/main/java/io/github/mannkostir/projections/NestOptions.java`:

```java
package io.github.mannkostir.projections;

import java.io.Serializable;
import java.time.Duration;
import java.util.Optional;

public final class NestOptions implements Serializable {
    private final Duration parentStateTtl;
    private final Duration orphanTimeout;

    private NestOptions(Duration parentStateTtl, Duration orphanTimeout) {
        this.parentStateTtl = parentStateTtl;
        this.orphanTimeout = orphanTimeout;
    }

    public static NestOptions defaults() {
        return builder().build();
    }

    public static Builder builder() {
        return new Builder();
    }

    Optional<Duration> parentStateTtl() {
        return Optional.ofNullable(parentStateTtl);
    }

    Optional<Duration> orphanTimeout() {
        return Optional.ofNullable(orphanTimeout);
    }

    public static final class Builder {
        private Duration parentStateTtl;
        private Duration orphanTimeout;

        private Builder() {
        }

        public Builder parentStateTtl(Duration ttl) {
            this.parentStateTtl = Durations.requirePositive(ttl, "parentStateTtl");
            return this;
        }

        public Builder orphanTimeout(Duration timeout) {
            this.orphanTimeout = Durations.requirePositive(timeout, "orphanTimeout");
            return this;
        }

        public NestOptions build() {
            return new NestOptions(parentStateTtl, orphanTimeout);
        }
    }
}
```

`src/main/java/io/github/mannkostir/projections/ChildOptions.java`:

```java
package io.github.mannkostir.projections;

import java.io.Serializable;
import java.time.Duration;
import java.util.Optional;

public final class ChildOptions implements Serializable {
    private final Duration stateTtl;

    private ChildOptions(Duration stateTtl) {
        this.stateTtl = stateTtl;
    }

    public static ChildOptions defaults() {
        return builder().build();
    }

    public static Builder builder() {
        return new Builder();
    }

    Optional<Duration> stateTtl() {
        return Optional.ofNullable(stateTtl);
    }

    public static final class Builder {
        private Duration stateTtl;

        private Builder() {
        }

        public Builder stateTtl(Duration ttl) {
            this.stateTtl = Durations.requirePositive(ttl, "stateTtl");
            return this;
        }

        public ChildOptions build() {
            return new ChildOptions(stateTtl);
        }
    }
}
```

`src/main/java/io/github/mannkostir/projections/LookupOptions.java`:

```java
package io.github.mannkostir.projections;

import java.io.Serializable;
import java.time.Duration;
import java.util.Optional;

public final class LookupOptions implements Serializable {
    private final Duration stateTtl;
    private final boolean requireMatch;

    private LookupOptions(Duration stateTtl, boolean requireMatch) {
        this.stateTtl = stateTtl;
        this.requireMatch = requireMatch;
    }

    public static LookupOptions defaults() {
        return builder().build();
    }

    public static Builder builder() {
        return new Builder();
    }

    Optional<Duration> stateTtl() {
        return Optional.ofNullable(stateTtl);
    }

    boolean requireMatch() {
        return requireMatch;
    }

    public static final class Builder {
        private Duration stateTtl;
        private boolean requireMatch;

        private Builder() {
        }

        public Builder stateTtl(Duration ttl) {
            this.stateTtl = Durations.requirePositive(ttl, "stateTtl");
            return this;
        }

        public Builder requireMatch(boolean requireMatch) {
            this.requireMatch = requireMatch;
            return this;
        }

        public LookupOptions build() {
            return new LookupOptions(stateTtl, requireMatch);
        }
    }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `mvn -q -s /private/tmp/claude-501/-Users-mannkostir-Documents-flink-use-cases-framework/3f51feaa-f321-4669-aad0-9b002a5b11e0/scratchpad/settings.xml test -Dtest=OptionsTest`
Expected: exit 0, no test failures.

- [ ] **Step 5: Run the whole suite**

Run: `mvn -q -s /private/tmp/claude-501/-Users-mannkostir-Documents-flink-use-cases-framework/3f51feaa-f321-4669-aad0-9b002a5b11e0/scratchpad/settings.xml clean verify`
Expected: exit 0.

- [ ] **Step 6: Commit**

```bash
git add src/main/java/io/github/mannkostir/projections/Durations.java src/main/java/io/github/mannkostir/projections/NestOptions.java src/main/java/io/github/mannkostir/projections/ChildOptions.java src/main/java/io/github/mannkostir/projections/LookupOptions.java src/test/java/io/github/mannkostir/projections/OptionsTest.java
git commit -m "add projection options"
```

---

### Task 7: Nest rules

**Files:**
- Create: `src/main/java/io/github/mannkostir/projections/ChildSlot.java`
- Create: `src/main/java/io/github/mannkostir/projections/Children.java`
- Create: `src/main/java/io/github/mannkostir/projections/Assembler.java`
- Create: `src/main/java/io/github/mannkostir/projections/SlotChange.java`
- Create: `src/main/java/io/github/mannkostir/projections/LevelState.java`
- Create: `src/main/java/io/github/mannkostir/projections/NestRules.java`
- Create (test helper): `src/test/java/io/github/mannkostir/projections/InMemoryLevelState.java`
- Test: `src/test/java/io/github/mannkostir/projections/NestRulesTest.java`

**Interfaces:**
- Consumes: `Change`, `Upsert`, `Delete` (Task 2).
- Produces: public `ChildSlot<C>` (package-private ctor `ChildSlot(String level, String name, int index)`, accessors `level()`, `name()`, `index()`); public `Children` (package-private ctor `Children(String level, List<List<Object>> slotValues)`, public `<C> List<C> get(ChildSlot<C>)`); public `Assembler<P,O>`; package-private `record SlotChange(int slot, Change<?> change)`; `interface LevelState<P,O>`; `NestRules<P,O>(String level, int slotCount, Assembler<P,O>)` with `onParent(String parentId, Change<P>, LevelState<P,O>, Consumer<Change<O>>)`, `onChild(String parentId, SlotChange, LevelState<P,O>, Consumer<Change<O>>)`, `onOrphanTimeout(LevelState<P,O>)`; test helper `InMemoryLevelState<P,O>(int slotCount)`.

- [ ] **Step 1: Write the failing tests**

`src/test/java/io/github/mannkostir/projections/InMemoryLevelState.java`:

```java
package io.github.mannkostir.projections;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

final class InMemoryLevelState<P, O> implements LevelState<P, O> {
    private final List<TreeMap<String, Object>> children = new ArrayList<>();
    private P parent;
    private O lastDoc;

    InMemoryLevelState(int slotCount) {
        for (int slot = 0; slot < slotCount; slot++) {
            children.add(new TreeMap<>());
        }
    }

    @Override
    public P parent() {
        return parent;
    }

    @Override
    public void putParent(P value) {
        parent = value;
    }

    @Override
    public boolean hasChild(int slot, String childId) {
        return children.get(slot).containsKey(childId);
    }

    @Override
    public void putChild(int slot, String childId, Object child) {
        children.get(slot).put(childId, child);
    }

    @Override
    public void removeChild(int slot, String childId) {
        children.get(slot).remove(childId);
    }

    @Override
    public List<Object> children(int slot) {
        return List.copyOf(children.get(slot).values());
    }

    @Override
    public O lastDoc() {
        return lastDoc;
    }

    @Override
    public void putLastDoc(O doc) {
        lastDoc = doc;
    }

    @Override
    public void clearChildren() {
        children.forEach(TreeMap::clear);
    }

    @Override
    public void clearAll() {
        parent = null;
        lastDoc = null;
        clearChildren();
    }

    int childCount() {
        return children.stream().mapToInt(TreeMap::size).sum();
    }
}
```

`src/test/java/io/github/mannkostir/projections/NestRulesTest.java`:

```java
package io.github.mannkostir.projections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

class NestRulesTest {
    private static final ChildSlot<String> SKILLS = new ChildSlot<>("candidate", "skills", 0);
    private static final ChildSlot<String> JOBS = new ChildSlot<>("candidate", "jobs", 1);

    private final NestRules<String, String> rules = new NestRules<>("candidate", 2,
            (parent, children) -> parent + "|" + children.get(SKILLS) + "|" + children.get(JOBS));
    private final InMemoryLevelState<String, String> state = new InMemoryLevelState<>(2);
    private final List<Change<String>> out = new ArrayList<>();

    @Test
    void childUpsertWithoutParentIsStoredSilently() throws Exception {
        rules.onChild("c1", skill(new Upsert<>("s1", "java")), state, out::add);

        assertThat(out).isEmpty();
        assertThat(state.children(0)).containsExactly("java");
    }

    @Test
    void childUpsertWithParentEmitsDocument() throws Exception {
        rules.onParent("c1", new Upsert<>("c1", "alice"), state, out::add);
        rules.onChild("c1", skill(new Upsert<>("s1", "java")), state, out::add);

        assertThat(out).last().isEqualTo(new Upsert<>("c1", "alice|[java]|[]"));
    }

    @Test
    void storedChildDeleteWithParentEmitsDocumentWithoutIt() throws Exception {
        rules.onParent("c1", new Upsert<>("c1", "alice"), state, out::add);
        rules.onChild("c1", skill(new Upsert<>("s1", "java")), state, out::add);
        rules.onChild("c1", skill(new Delete<>("s1", "java")), state, out::add);

        assertThat(out).last().isEqualTo(new Upsert<>("c1", "alice|[]|[]"));
    }

    @Test
    void unknownChildDeleteEmitsNothing() throws Exception {
        rules.onParent("c1", new Upsert<>("c1", "alice"), state, out::add);
        out.clear();
        rules.onChild("c1", skill(new Delete<>("s9", "rust")), state, out::add);

        assertThat(out).isEmpty();
    }

    @Test
    void storedChildDeleteWithoutParentRemovesSilently() throws Exception {
        rules.onChild("c1", skill(new Upsert<>("s1", "java")), state, out::add);
        rules.onChild("c1", skill(new Delete<>("s1", "java")), state, out::add);

        assertThat(out).isEmpty();
        assertThat(state.childCount()).isZero();
    }

    @Test
    void parentUpsertEmitsDocumentWithStoredChildren() throws Exception {
        rules.onChild("c1", job(new Upsert<>("j1", "acme")), state, out::add);
        rules.onParent("c1", new Upsert<>("c1", "alice"), state, out::add);

        assertThat(out).containsExactly(new Upsert<>("c1", "alice|[]|[acme]"));
    }

    @Test
    void parentDeleteEmitsDeleteWithLastDocumentAndClearsEverything() throws Exception {
        rules.onParent("c1", new Upsert<>("c1", "alice"), state, out::add);
        rules.onChild("c1", skill(new Upsert<>("s1", "java")), state, out::add);
        rules.onParent("c1", new Delete<>("c1", "alice"), state, out::add);

        assertThat(out).last().isEqualTo(new Delete<>("c1", "alice|[java]|[]"));
        assertThat(state.parent()).isNull();
        assertThat(state.lastDoc()).isNull();
        assertThat(state.childCount()).isZero();
    }

    @Test
    void parentDeleteWithoutParentClearsChildrenSilently() throws Exception {
        rules.onChild("c1", skill(new Upsert<>("s1", "java")), state, out::add);
        rules.onParent("c1", new Delete<>("c1", "alice"), state, out::add);

        assertThat(out).isEmpty();
        assertThat(state.childCount()).isZero();
    }

    @Test
    void childrenArePassedSortedById() throws Exception {
        rules.onChild("c1", skill(new Upsert<>("s2", "scala")), state, out::add);
        rules.onChild("c1", skill(new Upsert<>("s1", "java")), state, out::add);
        rules.onParent("c1", new Upsert<>("c1", "alice"), state, out::add);

        assertThat(out).containsExactly(new Upsert<>("c1", "alice|[java, scala]|[]"));
    }

    @Test
    void orphanTimeoutClearsChildrenWhenParentAbsent() throws Exception {
        rules.onChild("c1", skill(new Upsert<>("s1", "java")), state, out::add);
        rules.onOrphanTimeout(state);

        assertThat(state.childCount()).isZero();
    }

    @Test
    void orphanTimeoutKeepsChildrenWhenParentPresent() throws Exception {
        rules.onChild("c1", skill(new Upsert<>("s1", "java")), state, out::add);
        rules.onParent("c1", new Upsert<>("c1", "alice"), state, out::add);
        rules.onOrphanTimeout(state);

        assertThat(state.childCount()).isEqualTo(1);
    }

    @Test
    void childrenRejectSlotOfAnotherLevel() {
        Children children = new Children("candidate", List.of(List.of()));

        assertThatThrownBy(() -> children.get(new ChildSlot<String>("experience", "projects", 0)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("experience");
    }

    @Test
    void assemblerFailurePropagates() {
        NestRules<String, String> failing = new NestRules<>("candidate", 2, (parent, children) -> {
            throw new IllegalStateException("broken assembler");
        });

        assertThatThrownBy(() -> failing.onParent("c1", new Upsert<>("c1", "alice"), state, out::add))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("broken assembler");
    }

    @Test
    void childrenListIsUnmodifiable() throws Exception {
        NestRules<String, String> mutating = new NestRules<>("candidate", 2, (parent, children) -> {
            children.get(SKILLS).add("injected");
            return parent;
        });
        rules.onChild("c1", skill(new Upsert<>("s1", "java")), state, out::add);

        assertThatThrownBy(() -> mutating.onParent("c1", new Upsert<>("c1", "alice"), state, out::add))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThat(state.children(0)).containsExactly("java");
    }

    @Test
    void replayedUpsertEmitsIdenticalDocument() throws Exception {
        rules.onParent("c1", new Upsert<>("c1", "alice"), state, out::add);
        rules.onChild("c1", skill(new Upsert<>("s1", "java")), state, out::add);
        rules.onChild("c1", skill(new Upsert<>("s1", "java")), state, out::add);

        assertThat(out.get(2)).isEqualTo(out.get(1));
    }

    private static SlotChange skill(Change<String> change) {
        return new SlotChange(0, change);
    }

    private static SlotChange job(Change<String> change) {
        return new SlotChange(1, change);
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `mvn -q -s /private/tmp/claude-501/-Users-mannkostir-Documents-flink-use-cases-framework/3f51feaa-f321-4669-aad0-9b002a5b11e0/scratchpad/settings.xml test -Dtest=NestRulesTest`
Expected: BUILD FAILURE with compilation errors: cannot find symbol `NestRules` / `ChildSlot` / `SlotChange`.

- [ ] **Step 3: Write the implementation**

`src/main/java/io/github/mannkostir/projections/ChildSlot.java`:

```java
package io.github.mannkostir.projections;

import java.io.Serializable;

public final class ChildSlot<C> implements Serializable {
    private final String level;
    private final String name;
    private final int index;

    ChildSlot(String level, String name, int index) {
        this.level = level;
        this.name = name;
        this.index = index;
    }

    String level() {
        return level;
    }

    String name() {
        return name;
    }

    int index() {
        return index;
    }

    @Override
    public String toString() {
        return level + "." + name;
    }
}
```

`src/main/java/io/github/mannkostir/projections/Children.java`:

```java
package io.github.mannkostir.projections;

import java.util.List;

public final class Children {
    private final String level;
    private final List<List<Object>> slotValues;

    Children(String level, List<List<Object>> slotValues) {
        this.level = level;
        this.slotValues = slotValues;
    }

    @SuppressWarnings("unchecked")
    public <C> List<C> get(ChildSlot<C> slot) {
        if (!slot.level().equals(level)) {
            throw new IllegalArgumentException(
                    "Child slot '" + slot + "' belongs to level '" + slot.level() + "', not to level '" + level + "'");
        }
        return (List<C>) (List<?>) slotValues.get(slot.index());
    }
}
```

`src/main/java/io/github/mannkostir/projections/Assembler.java`:

```java
package io.github.mannkostir.projections;

import java.io.Serializable;

public interface Assembler<P, O> extends Serializable {
    O assemble(P parent, Children children) throws Exception;
}
```

`src/main/java/io/github/mannkostir/projections/SlotChange.java`:

```java
package io.github.mannkostir.projections;

record SlotChange(int slot, Change<?> change) {
}
```

`src/main/java/io/github/mannkostir/projections/LevelState.java`:

```java
package io.github.mannkostir.projections;

import java.util.List;

interface LevelState<P, O> {
    P parent() throws Exception;

    void putParent(P parent) throws Exception;

    boolean hasChild(int slot, String childId) throws Exception;

    void putChild(int slot, String childId, Object child) throws Exception;

    void removeChild(int slot, String childId) throws Exception;

    List<Object> children(int slot) throws Exception;

    O lastDoc() throws Exception;

    void putLastDoc(O doc) throws Exception;

    void clearChildren() throws Exception;

    void clearAll() throws Exception;
}
```

`src/main/java/io/github/mannkostir/projections/NestRules.java`:

```java
package io.github.mannkostir.projections;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

final class NestRules<P, O> implements Serializable {
    private final String level;
    private final int slotCount;
    private final Assembler<P, O> assembler;

    NestRules(String level, int slotCount, Assembler<P, O> assembler) {
        this.level = level;
        this.slotCount = slotCount;
        this.assembler = assembler;
    }

    void onParent(String parentId, Change<P> change, LevelState<P, O> state, Consumer<Change<O>> out) throws Exception {
        if (change instanceof Upsert<P> upsert) {
            state.putParent(upsert.value());
            emitAssembled(parentId, state, out);
            return;
        }
        deleteParent(parentId, state, out);
    }

    void onChild(String parentId, SlotChange slotChange, LevelState<P, O> state, Consumer<Change<O>> out) throws Exception {
        Change<?> change = slotChange.change();
        if (change instanceof Upsert<?>) {
            state.putChild(slotChange.slot(), change.id(), change.value());
            emitIfParentPresent(parentId, state, out);
            return;
        }
        if (state.hasChild(slotChange.slot(), change.id())) {
            state.removeChild(slotChange.slot(), change.id());
            emitIfParentPresent(parentId, state, out);
        }
    }

    void onOrphanTimeout(LevelState<P, O> state) throws Exception {
        if (state.parent() == null) {
            state.clearChildren();
        }
    }

    private void deleteParent(String parentId, LevelState<P, O> state, Consumer<Change<O>> out) throws Exception {
        if (state.parent() == null) {
            state.clearChildren();
            return;
        }
        O lastDoc = state.lastDoc();
        state.clearAll();
        out.accept(new Delete<>(parentId, lastDoc));
    }

    private void emitIfParentPresent(String parentId, LevelState<P, O> state, Consumer<Change<O>> out) throws Exception {
        if (state.parent() != null) {
            emitAssembled(parentId, state, out);
        }
    }

    private void emitAssembled(String parentId, LevelState<P, O> state, Consumer<Change<O>> out) throws Exception {
        O doc = assembler.assemble(state.parent(), children(state));
        state.putLastDoc(doc);
        out.accept(new Upsert<>(parentId, doc));
    }

    private Children children(LevelState<P, O> state) throws Exception {
        List<List<Object>> slotValues = new ArrayList<>(slotCount);
        for (int slot = 0; slot < slotCount; slot++) {
            slotValues.add(List.copyOf(state.children(slot)));
        }
        return new Children(level, List.copyOf(slotValues));
    }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `mvn -q -s /private/tmp/claude-501/-Users-mannkostir-Documents-flink-use-cases-framework/3f51feaa-f321-4669-aad0-9b002a5b11e0/scratchpad/settings.xml test -Dtest=NestRulesTest`
Expected: exit 0, no test failures.

- [ ] **Step 5: Run the whole suite**

Run: `mvn -q -s /private/tmp/claude-501/-Users-mannkostir-Documents-flink-use-cases-framework/3f51feaa-f321-4669-aad0-9b002a5b11e0/scratchpad/settings.xml clean verify`
Expected: exit 0.

- [ ] **Step 6: Commit**

```bash
git add src/main/java/io/github/mannkostir/projections/ChildSlot.java src/main/java/io/github/mannkostir/projections/Children.java src/main/java/io/github/mannkostir/projections/Assembler.java src/main/java/io/github/mannkostir/projections/SlotChange.java src/main/java/io/github/mannkostir/projections/LevelState.java src/main/java/io/github/mannkostir/projections/NestRules.java src/test/java/io/github/mannkostir/projections/InMemoryLevelState.java src/test/java/io/github/mannkostir/projections/NestRulesTest.java
git commit -m "add nest rules"
```

---

### Task 8: Tagged child union serialization

**Files:**
- Create: `src/main/java/io/github/mannkostir/projections/SlotChangeSerializer.java`
- Create: `src/main/java/io/github/mannkostir/projections/SlotChangeTypeInfo.java`
- Test: `src/test/java/io/github/mannkostir/projections/SlotChangeSerializerTest.java`

**Interfaces:**
- Consumes: `SlotChange` (Task 7); `ChangeSerializer` (Task 3).
- Produces: package-private `SlotChangeSerializer(TypeSerializer<Change<Object>>[] slotSerializers)` with public nested `Snapshot`; `SlotChangeTypeInfo(List<TypeInformation<?>> slotValueTypes)`.

- [ ] **Step 1: Write the failing test**

`src/test/java/io/github/mannkostir/projections/SlotChangeSerializerTest.java`:

```java
package io.github.mannkostir.projections;

import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;

class SlotChangeSerializerTest extends SerializerTestBase<SlotChange> {
    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    protected TypeSerializer<SlotChange> createSerializer() {
        return new SlotChangeSerializer(new TypeSerializer[] {
                new ChangeSerializer<>(StringSerializer.INSTANCE),
                new ChangeSerializer<>(LongSerializer.INSTANCE)
        });
    }

    @Override
    protected int getLength() {
        return -1;
    }

    @Override
    protected Class<SlotChange> getTypeClass() {
        return SlotChange.class;
    }

    @Override
    protected boolean allowNullInstances(TypeSerializer<SlotChange> serializer) {
        return true;
    }

    @Override
    protected SlotChange[] getTestData() {
        return new SlotChange[] {
                new SlotChange(0, new Upsert<>("s1", "java")),
                new SlotChange(0, new Delete<>("s1", "java")),
                new SlotChange(1, new Upsert<>("j1", 42L)),
                new SlotChange(1, new Delete<>("j2", -1L))
        };
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `mvn -q -s /private/tmp/claude-501/-Users-mannkostir-Documents-flink-use-cases-framework/3f51feaa-f321-4669-aad0-9b002a5b11e0/scratchpad/settings.xml test -Dtest=SlotChangeSerializerTest`
Expected: BUILD FAILURE with compilation error: cannot find symbol `SlotChangeSerializer`.

- [ ] **Step 3: Write the implementation**

`src/main/java/io/github/mannkostir/projections/SlotChangeSerializer.java`:

```java
package io.github.mannkostir.projections;

import java.io.IOException;
import java.util.Arrays;

import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

final class SlotChangeSerializer extends TypeSerializer<SlotChange> {
    private final TypeSerializer<Change<Object>>[] slotSerializers;

    SlotChangeSerializer(TypeSerializer<Change<Object>>[] slotSerializers) {
        this.slotSerializers = slotSerializers;
    }

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    @SuppressWarnings("unchecked")
    public TypeSerializer<SlotChange> duplicate() {
        TypeSerializer<Change<Object>>[] duplicated = new TypeSerializer[slotSerializers.length];
        boolean stateless = true;
        for (int slot = 0; slot < slotSerializers.length; slot++) {
            duplicated[slot] = slotSerializers[slot].duplicate();
            stateless &= duplicated[slot] == slotSerializers[slot];
        }
        return stateless ? this : new SlotChangeSerializer(duplicated);
    }

    @Override
    public SlotChange createInstance() {
        return null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public SlotChange copy(SlotChange from) {
        return new SlotChange(from.slot(), slotSerializers[from.slot()].copy((Change<Object>) from.change()));
    }

    @Override
    public SlotChange copy(SlotChange from, SlotChange reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void serialize(SlotChange record, DataOutputView target) throws IOException {
        target.writeInt(record.slot());
        slotSerializers[record.slot()].serialize((Change<Object>) record.change(), target);
    }

    @Override
    public SlotChange deserialize(DataInputView source) throws IOException {
        int slot = source.readInt();
        return new SlotChange(slot, slotSerializers[slot].deserialize(source));
    }

    @Override
    public SlotChange deserialize(SlotChange reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        int slot = source.readInt();
        target.writeInt(slot);
        slotSerializers[slot].copy(source, target);
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof SlotChangeSerializer that && Arrays.equals(slotSerializers, that.slotSerializers);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(slotSerializers);
    }

    @Override
    public TypeSerializerSnapshot<SlotChange> snapshotConfiguration() {
        return new Snapshot(this);
    }

    public static final class Snapshot extends CompositeTypeSerializerSnapshot<SlotChange, SlotChangeSerializer> {
        private static final int VERSION = 1;

        public Snapshot() {
        }

        Snapshot(SlotChangeSerializer serializer) {
            super(serializer);
        }

        @Override
        protected int getCurrentOuterSnapshotVersion() {
            return VERSION;
        }

        @Override
        protected TypeSerializer<?>[] getNestedSerializers(SlotChangeSerializer outerSerializer) {
            return outerSerializer.slotSerializers;
        }

        @Override
        @SuppressWarnings("unchecked")
        protected SlotChangeSerializer createOuterSerializerWithNestedSerializers(TypeSerializer<?>[] nestedSerializers) {
            TypeSerializer<Change<Object>>[] slots = new TypeSerializer[nestedSerializers.length];
            for (int slot = 0; slot < nestedSerializers.length; slot++) {
                slots[slot] = (TypeSerializer<Change<Object>>) nestedSerializers[slot];
            }
            return new SlotChangeSerializer(slots);
        }
    }
}
```

`src/main/java/io/github/mannkostir/projections/SlotChangeTypeInfo.java`:

```java
package io.github.mannkostir.projections;

import java.util.List;

import org.apache.flink.api.common.serialization.SerializerConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

final class SlotChangeTypeInfo extends TypeInformation<SlotChange> {
    private final List<TypeInformation<?>> slotValueTypes;

    SlotChangeTypeInfo(List<TypeInformation<?>> slotValueTypes) {
        this.slotValueTypes = List.copyOf(slotValueTypes);
    }

    @Override
    public boolean isBasicType() {
        return false;
    }

    @Override
    public boolean isTupleType() {
        return false;
    }

    @Override
    public int getArity() {
        return 1;
    }

    @Override
    public int getTotalFields() {
        return 1;
    }

    @Override
    public Class<SlotChange> getTypeClass() {
        return SlotChange.class;
    }

    @Override
    public boolean isKeyType() {
        return false;
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public TypeSerializer<SlotChange> createSerializer(SerializerConfig config) {
        TypeSerializer<Change<Object>>[] slots = new TypeSerializer[slotValueTypes.size()];
        for (int slot = 0; slot < slots.length; slot++) {
            slots[slot] = new ChangeSerializer<>((TypeSerializer<Object>) slotValueTypes.get(slot).createSerializer(config));
        }
        return new SlotChangeSerializer(slots);
    }

    @Override
    public String toString() {
        return "SlotChange" + slotValueTypes;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof SlotChangeTypeInfo that && that.canEqual(this) && slotValueTypes.equals(that.slotValueTypes);
    }

    @Override
    public int hashCode() {
        return slotValueTypes.hashCode();
    }

    @Override
    public boolean canEqual(Object other) {
        return other instanceof SlotChangeTypeInfo;
    }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `mvn -q -s /private/tmp/claude-501/-Users-mannkostir-Documents-flink-use-cases-framework/3f51feaa-f321-4669-aad0-9b002a5b11e0/scratchpad/settings.xml test -Dtest=SlotChangeSerializerTest`
Expected: exit 0, no test failures.

- [ ] **Step 5: Run the whole suite**

Run: `mvn -q -s /private/tmp/claude-501/-Users-mannkostir-Documents-flink-use-cases-framework/3f51feaa-f321-4669-aad0-9b002a5b11e0/scratchpad/settings.xml clean verify`
Expected: exit 0.

- [ ] **Step 6: Commit**

```bash
git add src/main/java/io/github/mannkostir/projections/SlotChangeSerializer.java src/main/java/io/github/mannkostir/projections/SlotChangeTypeInfo.java src/test/java/io/github/mannkostir/projections/SlotChangeSerializerTest.java
git commit -m "add slot change serializer"
```

---

### Task 9: Nest operator

**Files:**
- Create: `src/main/java/io/github/mannkostir/projections/SlotSpec.java`
- Create: `src/main/java/io/github/mannkostir/projections/FlinkLevelState.java`
- Create: `src/main/java/io/github/mannkostir/projections/NestFunction.java`
- Create: `src/main/java/io/github/mannkostir/projections/SlotParentKey.java`
- Create: `src/main/java/io/github/mannkostir/projections/ChangeId.java`
- Test: `src/test/java/io/github/mannkostir/projections/NestFunctionTest.java`

**Interfaces:**
- Consumes: `NestRules`, `LevelState`, `SlotChange`, `ChildSlot` (Task 7); `NestOptions`, `ChildOptions` (Task 6); `StateTtl` (Task 5); `ContractNames` (Task 4); `ChangeSerializer` (Task 3).
- Produces: package-private `record SlotSpec(String name, TypeInformation<?> valueType, ChildOptions options)`; `NestFunction<P,O>(String level, TypeInformation<P> parentType, List<SlotSpec> slots, Assembler<P,O>, TypeInformation<O> docType, NestOptions)`, a `KeyedCoProcessFunction<String, Change<P>, SlotChange, Change<O>>`; `SlotParentKey(String level, List<String> slotNames, List<KeySelector<Object,String>> parentKeys)`; `ChangeId<T>` (`KeySelector<Change<T>,String>` returning `change.id()`).

- [ ] **Step 1: Write the failing test**

`src/test/java/io/github/mannkostir/projections/NestFunctionTest.java`:

```java
package io.github.mannkostir.projections;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

class NestFunctionTest {
    private static final ChildSlot<String> SKILLS = new ChildSlot<>("candidate", "skills", 0);

    @Test
    void assemblesParentWithChildren() throws Exception {
        try (var harness = harness(NestOptions.defaults(), ChildOptions.defaults())) {
            harness.open();
            harness.processElement2(skill(new Upsert<>("s1", "java@c1")), 1L);
            harness.processElement1(new Upsert<>("c1", "alice"), 2L);

            assertThat(harness.extractOutputValues()).containsExactly(new Upsert<>("c1", "alice[java@c1]"));
        }
    }

    @Test
    void parentDeleteCarriesLastDocument() throws Exception {
        try (var harness = harness(NestOptions.defaults(), ChildOptions.defaults())) {
            harness.open();
            harness.processElement1(new Upsert<>("c1", "alice"), 1L);
            harness.processElement2(skill(new Upsert<>("s1", "java@c1")), 2L);
            harness.processElement1(new Delete<>("c1", "alice"), 3L);

            assertThat(harness.extractOutputValues()).last().isEqualTo(new Delete<>("c1", "alice[java@c1]"));
        }
    }

    @Test
    void restoresStateFromSavepoint() throws Exception {
        OperatorSubtaskState snapshot;
        try (var harness = harness(NestOptions.defaults(), ChildOptions.defaults())) {
            harness.open();
            harness.processElement1(new Upsert<>("c1", "alice"), 1L);
            harness.processElement2(skill(new Upsert<>("s1", "java@c1")), 2L);
            snapshot = harness.snapshot(1L, 2L);
        }

        try (var restored = harness(NestOptions.defaults(), ChildOptions.defaults())) {
            restored.initializeState(snapshot);
            restored.open();
            restored.processElement2(skill(new Upsert<>("s2", "scala@c1")), 3L);
            restored.processElement1(new Delete<>("c1", "alice"), 4L);

            assertThat(restored.extractOutputValues()).containsExactly(
                    new Upsert<>("c1", "alice[java@c1, scala@c1]"),
                    new Delete<>("c1", "alice[java@c1, scala@c1]"));
        }
    }

    @Test
    void registersStateUnderContractNames() throws Exception {
        try (var harness = harness(NestOptions.defaults(), ChildOptions.defaults())) {
            harness.open();
            harness.processElement1(new Upsert<>("c1", "alice"), 1L);
            harness.processElement2(skill(new Upsert<>("s1", "java@c1")), 2L);

            var backend = harness.getOperator().<String>getKeyedStateBackend();
            assertThat(backend.getKeys("candidate.parent", VoidNamespace.INSTANCE)).containsExactly("c1");
            assertThat(backend.getKeys("candidate.child.skills", VoidNamespace.INSTANCE)).containsExactly("c1");
            assertThat(backend.getKeys("candidate.last-doc", VoidNamespace.INSTANCE)).containsExactly("c1");
        }
    }

    @Test
    void orphanTimeoutDropsChildrenOfAbsentParent() throws Exception {
        NestOptions options = NestOptions.builder().orphanTimeout(Duration.ofMillis(100)).build();
        try (var harness = harness(options, ChildOptions.defaults())) {
            harness.open();
            harness.setProcessingTime(0L);
            harness.processElement2(skill(new Upsert<>("s1", "java@c1")), 1L);
            harness.setProcessingTime(200L);
            harness.processElement1(new Upsert<>("c1", "alice"), 2L);

            assertThat(harness.extractOutputValues()).containsExactly(new Upsert<>("c1", "alice[]"));
        }
    }

    @Test
    void childStateExpiresAfterTtl() throws Exception {
        ChildOptions childOptions = ChildOptions.builder().stateTtl(Duration.ofMillis(100)).build();
        try (var harness = harness(NestOptions.defaults(), childOptions)) {
            harness.open();
            harness.setStateTtlProcessingTime(0L);
            harness.processElement2(skill(new Upsert<>("s1", "java@c1")), 1L);
            harness.setStateTtlProcessingTime(200L);
            harness.processElement1(new Upsert<>("c1", "alice"), 2L);

            assertThat(harness.extractOutputValues()).containsExactly(new Upsert<>("c1", "alice[]"));
        }
    }

    @Test
    void parentStateExpiresAfterTtl() throws Exception {
        NestOptions options = NestOptions.builder().parentStateTtl(Duration.ofMillis(100)).build();
        try (var harness = harness(options, ChildOptions.defaults())) {
            harness.open();
            harness.setStateTtlProcessingTime(0L);
            harness.processElement1(new Upsert<>("c1", "alice"), 1L);
            harness.setStateTtlProcessingTime(200L);
            harness.processElement2(skill(new Upsert<>("s1", "java@c1")), 2L);

            assertThat(harness.extractOutputValues()).containsExactly(new Upsert<>("c1", "alice[]"));
        }
    }

    @Test
    void restoresWhenSlotIsAdded() throws Exception {
        OperatorSubtaskState snapshot;
        try (var harness = harness(NestOptions.defaults(), ChildOptions.defaults())) {
            harness.open();
            harness.processElement1(new Upsert<>("c1", "alice"), 1L);
            harness.processElement2(skill(new Upsert<>("s1", "java@c1")), 2L);
            snapshot = harness.snapshot(1L, 2L);
        }

        ChildSlot<String> jobs = new ChildSlot<>("candidate", "jobs", 1);
        NestFunction<String, String> widened = new NestFunction<>(
                "candidate",
                Types.STRING,
                List.of(new SlotSpec("skills", Types.STRING, ChildOptions.defaults()),
                        new SlotSpec("jobs", Types.STRING, ChildOptions.defaults())),
                (parent, children) -> parent + children.get(SKILLS) + children.get(jobs),
                Types.STRING,
                NestOptions.defaults());
        KeySelector<Object, String> parentOf = value -> ((String) value).split("@")[1];
        try (var restored = new KeyedTwoInputStreamOperatorTestHarness<String, Change<String>, SlotChange, Change<String>>(
                new KeyedCoProcessOperator<>(widened),
                new ChangeId<>(),
                new SlotParentKey("candidate", List.of("skills", "jobs"), List.of(parentOf, parentOf)),
                Types.STRING)) {
            restored.setup(new ChangeSerializer<>(StringSerializer.INSTANCE));
            restored.initializeState(snapshot);
            restored.open();
            restored.processElement2(new SlotChange(1, new Upsert<>("j1", "acme@c1")), 3L);

            assertThat(restored.extractOutputValues()).containsExactly(new Upsert<>("c1", "alice[java@c1][acme@c1]"));
        }
    }

    private static SlotChange skill(Change<String> change) {
        return new SlotChange(0, change);
    }

    private static KeyedTwoInputStreamOperatorTestHarness<String, Change<String>, SlotChange, Change<String>> harness(
            NestOptions options, ChildOptions childOptions) throws Exception {
        NestFunction<String, String> function = new NestFunction<>(
                "candidate",
                Types.STRING,
                List.of(new SlotSpec("skills", Types.STRING, childOptions)),
                (parent, children) -> parent + children.get(SKILLS),
                Types.STRING,
                options);
        KeySelector<Object, String> skillParent = value -> ((String) value).split("@")[1];
        var harness = new KeyedTwoInputStreamOperatorTestHarness<String, Change<String>, SlotChange, Change<String>>(
                new KeyedCoProcessOperator<>(function),
                new ChangeId<>(),
                new SlotParentKey("candidate", List.of("skills"), List.of(skillParent)),
                Types.STRING);
        harness.setup(new ChangeSerializer<>(StringSerializer.INSTANCE));
        return harness;
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `mvn -q -s /private/tmp/claude-501/-Users-mannkostir-Documents-flink-use-cases-framework/3f51feaa-f321-4669-aad0-9b002a5b11e0/scratchpad/settings.xml test -Dtest=NestFunctionTest`
Expected: BUILD FAILURE with compilation errors: cannot find symbol `NestFunction` / `SlotSpec` / `SlotParentKey` / `ChangeId`.

- [ ] **Step 3: Write the implementation**

`src/main/java/io/github/mannkostir/projections/SlotSpec.java`:

```java
package io.github.mannkostir.projections;

import java.io.Serializable;

import org.apache.flink.api.common.typeinfo.TypeInformation;

record SlotSpec(String name, TypeInformation<?> valueType, ChildOptions options) implements Serializable {
}
```

`src/main/java/io/github/mannkostir/projections/FlinkLevelState.java`:

```java
package io.github.mannkostir.projections;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.ValueState;

final class FlinkLevelState<P, O> implements LevelState<P, O> {
    private final ValueState<P> parent;
    private final List<MapState<String, Object>> children;
    private final ValueState<O> lastDoc;

    FlinkLevelState(ValueState<P> parent, List<MapState<String, Object>> children, ValueState<O> lastDoc) {
        this.parent = parent;
        this.children = children;
        this.lastDoc = lastDoc;
    }

    @Override
    public P parent() throws Exception {
        return parent.value();
    }

    @Override
    public void putParent(P value) throws Exception {
        parent.update(value);
    }

    @Override
    public boolean hasChild(int slot, String childId) throws Exception {
        return children.get(slot).contains(childId);
    }

    @Override
    public void putChild(int slot, String childId, Object child) throws Exception {
        children.get(slot).put(childId, child);
    }

    @Override
    public void removeChild(int slot, String childId) throws Exception {
        children.get(slot).remove(childId);
    }

    @Override
    public List<Object> children(int slot) throws Exception {
        List<Map.Entry<String, Object>> entries = new ArrayList<>();
        for (Map.Entry<String, Object> entry : children.get(slot).entries()) {
            entries.add(Map.entry(entry.getKey(), entry.getValue()));
        }
        entries.sort(Map.Entry.comparingByKey());
        return entries.stream().map(Map.Entry::getValue).toList();
    }

    @Override
    public O lastDoc() throws Exception {
        return lastDoc.value();
    }

    @Override
    public void putLastDoc(O doc) throws Exception {
        lastDoc.update(doc);
    }

    @Override
    public void clearChildren() throws Exception {
        for (MapState<String, Object> slot : children) {
            slot.clear();
        }
    }

    @Override
    public void clearAll() throws Exception {
        parent.clear();
        lastDoc.clear();
        clearChildren();
    }
}
```

`src/main/java/io/github/mannkostir/projections/NestFunction.java`:

```java
package io.github.mannkostir.projections;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

final class NestFunction<P, O> extends KeyedCoProcessFunction<String, Change<P>, SlotChange, Change<O>> {
    private final String level;
    private final TypeInformation<P> parentType;
    private final TypeInformation<O> docType;
    private final List<SlotSpec> slots;
    private final NestOptions options;
    private final NestRules<P, O> rules;
    private transient LevelState<P, O> state;

    NestFunction(
            String level,
            TypeInformation<P> parentType,
            List<SlotSpec> slots,
            Assembler<P, O> assembler,
            TypeInformation<O> docType,
            NestOptions options) {
        this.level = level;
        this.parentType = parentType;
        this.docType = docType;
        this.slots = List.copyOf(slots);
        this.options = options;
        this.rules = new NestRules<>(level, slots.size(), assembler);
    }

    @Override
    public void open(OpenContext openContext) {
        state = new FlinkLevelState<>(
                getRuntimeContext().getState(StateTtl.applyTo(
                        new ValueStateDescriptor<>(ContractNames.nestParentState(level), parentType),
                        options.parentStateTtl())),
                childStates(),
                getRuntimeContext().getState(StateTtl.applyTo(
                        new ValueStateDescriptor<>(ContractNames.nestLastDocState(level), docType),
                        options.parentStateTtl())));
    }

    @Override
    public void processElement1(Change<P> change, Context context, Collector<Change<O>> out) throws Exception {
        rules.onParent(context.getCurrentKey(), change, state, out::collect);
    }

    @Override
    public void processElement2(SlotChange change, Context context, Collector<Change<O>> out) throws Exception {
        rules.onChild(context.getCurrentKey(), change, state, out::collect);
        if (change.change() instanceof Upsert<?> && state.parent() == null) {
            scheduleOrphanCheck(context);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext context, Collector<Change<O>> out) throws Exception {
        rules.onOrphanTimeout(state);
    }

    private void scheduleOrphanCheck(Context context) {
        options.orphanTimeout().map(Duration::toMillis).ifPresent(timeout -> context.timerService()
                .registerProcessingTimeTimer(context.timerService().currentProcessingTime() + timeout));
    }

    @SuppressWarnings("unchecked")
    private List<MapState<String, Object>> childStates() {
        List<MapState<String, Object>> childStates = new ArrayList<>(slots.size());
        for (SlotSpec slot : slots) {
            MapStateDescriptor<String, Object> descriptor = new MapStateDescriptor<>(
                    ContractNames.nestChildState(level, slot.name()),
                    Types.STRING,
                    (TypeInformation<Object>) slot.valueType());
            childStates.add(getRuntimeContext().getMapState(StateTtl.applyTo(descriptor, slot.options().stateTtl())));
        }
        return childStates;
    }
}
```

`src/main/java/io/github/mannkostir/projections/SlotParentKey.java`:

```java
package io.github.mannkostir.projections;

import java.util.List;

import org.apache.flink.api.java.functions.KeySelector;

final class SlotParentKey implements KeySelector<SlotChange, String> {
    private final String level;
    private final List<String> slotNames;
    private final List<KeySelector<Object, String>> parentKeys;

    SlotParentKey(String level, List<String> slotNames, List<KeySelector<Object, String>> parentKeys) {
        this.level = level;
        this.slotNames = List.copyOf(slotNames);
        this.parentKeys = List.copyOf(parentKeys);
    }

    @Override
    public String getKey(SlotChange slotChange) throws Exception {
        String parentKey = parentKeys.get(slotChange.slot()).getKey(slotChange.change().value());
        if (parentKey == null) {
            throw new IllegalStateException("Nest '" + level + "' slot '" + slotNames.get(slotChange.slot())
                    + "': parent key selector returned null for child id '" + slotChange.change().id() + "'");
        }
        return parentKey;
    }
}
```

`src/main/java/io/github/mannkostir/projections/ChangeId.java`:

```java
package io.github.mannkostir.projections;

import org.apache.flink.api.java.functions.KeySelector;

final class ChangeId<T> implements KeySelector<Change<T>, String> {
    @Override
    public String getKey(Change<T> change) {
        return change.id();
    }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `mvn -q -s /private/tmp/claude-501/-Users-mannkostir-Documents-flink-use-cases-framework/3f51feaa-f321-4669-aad0-9b002a5b11e0/scratchpad/settings.xml test -Dtest=NestFunctionTest`
Expected: exit 0, no test failures.

- [ ] **Step 5: Run the whole suite**

Run: `mvn -q -s /private/tmp/claude-501/-Users-mannkostir-Documents-flink-use-cases-framework/3f51feaa-f321-4669-aad0-9b002a5b11e0/scratchpad/settings.xml clean verify`
Expected: exit 0.

- [ ] **Step 6: Commit**

```bash
git add src/main/java/io/github/mannkostir/projections/SlotSpec.java src/main/java/io/github/mannkostir/projections/FlinkLevelState.java src/main/java/io/github/mannkostir/projections/NestFunction.java src/main/java/io/github/mannkostir/projections/SlotParentKey.java src/main/java/io/github/mannkostir/projections/ChangeId.java src/test/java/io/github/mannkostir/projections/NestFunctionTest.java
git commit -m "add nest operator"
```

---

### Task 10: Nest builder

**Files:**
- Create: `src/main/java/io/github/mannkostir/projections/SlotTagger.java`
- Create: `src/main/java/io/github/mannkostir/projections/Nest.java`
- Test: `src/test/java/io/github/mannkostir/projections/NestTest.java`

**Interfaces:**
- Consumes: everything from Tasks 2-9; `ChangesTest.environment()` (Task 4).
- Produces: public `Nest<P>`: `parent(String name, DataStream<Change<P>>, TypeInformation<P>)`, `withOptions(NestOptions)`, `child(String slot, DataStream<Change<C>>, KeySelector<C,String> parentKey, TypeInformation<C>[, ChildOptions])` returning `ChildSlot<C>`, `assemble(Assembler<P,O>, TypeInformation<O>)` returning `DataStream<Change<O>>`; test helpers `NestTest.changes(env, Change<String>...)` and `NestTest.uidsUpstreamOf(Transformation<?>)`, reused by Task 12.

- [ ] **Step 1: Write the failing test**

`src/test/java/io/github/mannkostir/projections/NestTest.java`:

```java
package io.github.mannkostir.projections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class NestTest {
    @RegisterExtension
    static final MiniClusterExtension CLUSTER = new MiniClusterExtension();

    @Test
    void assemblesDocumentFromTwoChildSlots() throws Exception {
        StreamExecutionEnvironment env = ChangesTest.environment();
        Nest<String> candidates = Nest.parent("candidate", changes(env, new Upsert<>("c1", "alice")), Types.STRING);
        ChildSlot<String> skills = candidates.child("skills", changes(env, new Upsert<>("s1", "java@c1")), NestTest::parentOf, Types.STRING);
        ChildSlot<String> jobs = candidates.child("jobs", changes(env, new Upsert<>("j1", "acme@c1")), NestTest::parentOf, Types.STRING);

        DataStream<Change<String>> documents = candidates.assemble(
                (parent, children) -> parent + children.get(skills) + children.get(jobs), Types.STRING);

        assertThat(documents.executeAndCollect(10)).last().isEqualTo(new Upsert<>("c1", "alice[java@c1][acme@c1]"));
    }

    @Test
    void namesOperatorsAfterLevelAndSlots() {
        StreamExecutionEnvironment env = ChangesTest.environment();
        Nest<String> candidates = Nest.parent("candidate", changes(env, new Upsert<>("c1", "alice")), Types.STRING);
        candidates.child("skills", changes(env, new Upsert<>("s1", "java@c1")), NestTest::parentOf, Types.STRING);

        DataStream<Change<String>> documents = candidates.assemble((parent, children) -> parent, Types.STRING);

        assertThat(documents.getTransformation().getUid()).isEqualTo("nest_candidate");
        assertThat(uidsUpstreamOf(documents.getTransformation())).contains("nest_candidate_route_skills");
    }

    @Test
    void rejectsInvalidLevelName() {
        StreamExecutionEnvironment env = ChangesTest.environment();

        assertThatThrownBy(() -> Nest.parent("Candidate", changes(env, new Upsert<>("c1", "alice")), Types.STRING))
                .isInstanceOf(ProjectionConfigurationException.class)
                .hasMessageContaining("'Candidate'");
    }

    @Test
    void rejectsDuplicateSlotName() {
        StreamExecutionEnvironment env = ChangesTest.environment();
        Nest<String> candidates = Nest.parent("candidate", changes(env, new Upsert<>("c1", "alice")), Types.STRING);
        candidates.child("skills", changes(env, new Upsert<>("s1", "java@c1")), NestTest::parentOf, Types.STRING);

        assertThatThrownBy(() -> candidates.child("skills", changes(env, new Upsert<>("s2", "go@c1")), NestTest::parentOf, Types.STRING))
                .isInstanceOf(ProjectionConfigurationException.class)
                .hasMessageContaining("'skills'");
    }

    @Test
    void rejectsAssembleWithoutChildSlots() {
        StreamExecutionEnvironment env = ChangesTest.environment();
        Nest<String> candidates = Nest.parent("candidate", changes(env, new Upsert<>("c1", "alice")), Types.STRING);

        assertThatThrownBy(() -> candidates.assemble((parent, children) -> parent, Types.STRING))
                .isInstanceOf(ProjectionConfigurationException.class)
                .hasMessageContaining("child(...)");
    }

    @Test
    void rejectsSecondAssemble() {
        StreamExecutionEnvironment env = ChangesTest.environment();
        Nest<String> candidates = Nest.parent("candidate", changes(env, new Upsert<>("c1", "alice")), Types.STRING);
        candidates.child("skills", changes(env, new Upsert<>("s1", "java@c1")), NestTest::parentOf, Types.STRING);
        candidates.assemble((parent, children) -> parent, Types.STRING);

        assertThatThrownBy(() -> candidates.assemble((parent, children) -> parent, Types.STRING))
                .isInstanceOf(ProjectionConfigurationException.class)
                .hasMessageContaining("already assembled");
    }

    static String parentOf(String child) {
        return child.split("@")[1];
    }

    @SafeVarargs
    static DataStream<Change<String>> changes(StreamExecutionEnvironment env, Change<String>... changes) {
        return env.fromData(List.of(changes), Changes.typeInfo(Types.STRING));
    }

    static List<String> uidsUpstreamOf(Transformation<?> transformation) {
        return transformation.getTransitivePredecessors().stream()
                .map(Transformation::getUid)
                .filter(uid -> uid != null)
                .toList();
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `mvn -q -s /private/tmp/claude-501/-Users-mannkostir-Documents-flink-use-cases-framework/3f51feaa-f321-4669-aad0-9b002a5b11e0/scratchpad/settings.xml test -Dtest=NestTest`
Expected: BUILD FAILURE with compilation errors: cannot find symbol `Nest` / `SlotTagger`.

- [ ] **Step 3: Write the implementation**

`src/main/java/io/github/mannkostir/projections/SlotTagger.java`:

```java
package io.github.mannkostir.projections;

final class SlotTagger<C> implements ChangeTagger<C, SlotChange> {
    private final int slot;

    SlotTagger(int slot) {
        this.slot = slot;
    }

    @Override
    public SlotChange tag(Routed<C> routed) {
        return new SlotChange(slot, routed.change());
    }
}
```

`src/main/java/io/github/mannkostir/projections/Nest.java`:

```java
package io.github.mannkostir.projections;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;

public final class Nest<P> {
    private final String name;
    private final DataStream<Change<P>> parents;
    private final TypeInformation<P> parentType;
    private final List<SlotDefinition<?>> slots = new ArrayList<>();
    private NestOptions options = NestOptions.defaults();
    private boolean assembled;

    private Nest(String name, DataStream<Change<P>> parents, TypeInformation<P> parentType) {
        this.name = name;
        this.parents = parents;
        this.parentType = parentType;
    }

    public static <P> Nest<P> parent(String name, DataStream<Change<P>> parents, TypeInformation<P> type) {
        return new Nest<>(Names.requireValid(name, "Nest name"), parents, type);
    }

    public Nest<P> withOptions(NestOptions options) {
        this.options = Objects.requireNonNull(options, "options");
        return this;
    }

    public <C> ChildSlot<C> child(
            String slot, DataStream<Change<C>> children, KeySelector<C, String> parentKey, TypeInformation<C> type) {
        return child(slot, children, parentKey, type, ChildOptions.defaults());
    }

    public <C> ChildSlot<C> child(
            String slot,
            DataStream<Change<C>> children,
            KeySelector<C, String> parentKey,
            TypeInformation<C> type,
            ChildOptions options) {
        requireNotAssembled();
        requireUniqueSlot(Names.requireValid(slot, "Child slot name"));
        ChildSlot<C> handle = new ChildSlot<>(name, slot, slots.size());
        slots.add(new SlotDefinition<>(handle, children, parentKey, type, Objects.requireNonNull(options, "options")));
        return handle;
    }

    public <O> DataStream<Change<O>> assemble(Assembler<P, O> assembler, TypeInformation<O> type) {
        requireNotAssembled();
        if (slots.isEmpty()) {
            throw new ProjectionConfigurationException(
                    "Nest '" + name + "' has no child slots: call child(...) at least once before assemble(...)");
        }
        assembled = true;
        String uid = ContractNames.nestUid(name);
        return parents.connect(routedChildren())
                .keyBy(new ChangeId<>(), new SlotParentKey(name, slotNames(), parentKeys()), Types.STRING)
                .process(new NestFunction<>(name, parentType, slotSpecs(), assembler, type, options), Changes.typeInfo(type))
                .uid(uid)
                .name(uid);
    }

    private DataStream<SlotChange> routedChildren() {
        SlotChangeTypeInfo slotChangeType = new SlotChangeTypeInfo(slots.stream()
                .<TypeInformation<?>>map(SlotDefinition::type)
                .toList());
        List<DataStream<SlotChange>> routed = slots.stream()
                .map(slot -> slot.route(name, slotChangeType))
                .toList();
        DataStream<SlotChange> first = routed.get(0);
        return routed.size() == 1 ? first : first.union(routed.subList(1, routed.size()).toArray(DataStream[]::new));
    }

    private List<String> slotNames() {
        return slots.stream().map(slot -> slot.handle().name()).toList();
    }

    private List<KeySelector<Object, String>> parentKeys() {
        return slots.stream().map(SlotDefinition::erasedParentKey).toList();
    }

    private List<SlotSpec> slotSpecs() {
        return slots.stream().map(slot -> new SlotSpec(slot.handle().name(), slot.type(), slot.options())).toList();
    }

    private void requireNotAssembled() {
        if (assembled) {
            throw new ProjectionConfigurationException(
                    "Nest '" + name + "' is already assembled: build a new Nest for another level");
        }
    }

    private void requireUniqueSlot(String slot) {
        if (slots.stream().anyMatch(existing -> existing.handle().name().equals(slot))) {
            throw new ProjectionConfigurationException(
                    "Nest '" + name + "' already has a child slot named '" + slot + "': slot names must be unique within a level");
        }
    }

    private record SlotDefinition<C>(
            ChildSlot<C> handle,
            DataStream<Change<C>> stream,
            KeySelector<C, String> parentKey,
            TypeInformation<C> type,
            ChildOptions options) {

        DataStream<SlotChange> route(String level, SlotChangeTypeInfo slotChangeType) {
            String uid = ContractNames.nestRouteUid(level, handle.name());
            RoutingFunction<C, SlotChange> routing = new RoutingFunction<>(
                    ContractNames.nestRouteState(level, handle.name()),
                    parentKey,
                    type,
                    options.stateTtl(),
                    new SlotTagger<>(handle.index()));
            return stream.keyBy(new ChangeId<>(), Types.STRING)
                    .process(routing, slotChangeType)
                    .uid(uid)
                    .name(uid);
        }

        @SuppressWarnings("unchecked")
        KeySelector<Object, String> erasedParentKey() {
            return (KeySelector<Object, String>) (KeySelector<?, String>) parentKey;
        }
    }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `mvn -q -s /private/tmp/claude-501/-Users-mannkostir-Documents-flink-use-cases-framework/3f51feaa-f321-4669-aad0-9b002a5b11e0/scratchpad/settings.xml test -Dtest=NestTest`
Expected: exit 0, no test failures.

- [ ] **Step 5: Run the whole suite**

Run: `mvn -q -s /private/tmp/claude-501/-Users-mannkostir-Documents-flink-use-cases-framework/3f51feaa-f321-4669-aad0-9b002a5b11e0/scratchpad/settings.xml clean verify`
Expected: exit 0.

- [ ] **Step 6: Commit**

```bash
git add src/main/java/io/github/mannkostir/projections/SlotTagger.java src/main/java/io/github/mannkostir/projections/Nest.java src/test/java/io/github/mannkostir/projections/NestTest.java
git commit -m "add nest builder"
```

---

### Task 11: Lookup rules and entity serialization

**Files:**
- Create: `src/main/java/io/github/mannkostir/projections/Enricher.java`
- Create: `src/main/java/io/github/mannkostir/projections/LookupEntity.java`
- Create: `src/main/java/io/github/mannkostir/projections/LookupState.java`
- Create: `src/main/java/io/github/mannkostir/projections/LookupRules.java`
- Create: `src/main/java/io/github/mannkostir/projections/LookupEntitySerializer.java`
- Create: `src/main/java/io/github/mannkostir/projections/LookupEntityTypeInfo.java`
- Create (test helper): `src/test/java/io/github/mannkostir/projections/InMemoryLookupState.java`
- Test: `src/test/java/io/github/mannkostir/projections/LookupRulesTest.java`
- Test: `src/test/java/io/github/mannkostir/projections/LookupEntitySerializerTest.java`

**Interfaces:**
- Consumes: `Change`, `Upsert`, `Delete` (Task 2); `ChangeSerializer` (Task 3).
- Produces: public `Enricher<E,D,O>`; package-private `record LookupEntity<E>(Change<E> change, boolean relocation)`; `interface LookupState<E,D>`; `LookupRules<E,D,O>(Enricher<E,D,O>, boolean requireMatch)` with `onEntity(LookupEntity<E>, LookupState<E,D>, Consumer<Change<O>>)` and `onDimension(Change<D>, LookupState<E,D>, Consumer<Change<O>>)`; `LookupEntitySerializer<E>(TypeSerializer<Change<E>>)` with public nested `Snapshot`; `LookupEntityTypeInfo<E>(TypeInformation<E>)`; test helper `InMemoryLookupState<E,D>`.

- [ ] **Step 1: Write the failing tests**

`src/test/java/io/github/mannkostir/projections/InMemoryLookupState.java`:

```java
package io.github.mannkostir.projections;

import java.util.SortedMap;
import java.util.TreeMap;

final class InMemoryLookupState<E, D> implements LookupState<E, D> {
    private final TreeMap<String, E> entities = new TreeMap<>();
    private D dimension;

    @Override
    public D dimension() {
        return dimension;
    }

    @Override
    public void putDimension(D value) {
        dimension = value;
    }

    @Override
    public void clearDimension() {
        dimension = null;
    }

    @Override
    public E entity(String entityId) {
        return entities.get(entityId);
    }

    @Override
    public void putEntity(String entityId, E entity) {
        entities.put(entityId, entity);
    }

    @Override
    public void removeEntity(String entityId) {
        entities.remove(entityId);
    }

    @Override
    public SortedMap<String, E> entities() {
        return new TreeMap<>(entities);
    }
}
```

`src/test/java/io/github/mannkostir/projections/LookupRulesTest.java`:

```java
package io.github.mannkostir.projections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

class LookupRulesTest {
    private final InMemoryLookupState<String, String> state = new InMemoryLookupState<>();
    private final List<Change<String>> out = new ArrayList<>();

    @Test
    void entityUpsertWithoutDimensionEmitsWithNull() throws Exception {
        leftJoin().onEntity(entity(new Upsert<>("e1", "dev")), state, out::add);

        assertThat(out).containsExactly(new Upsert<>("e1", "dev@null"));
    }

    @Test
    void entityUpsertWithDimensionEmitsEnriched() throws Exception {
        leftJoin().onDimension(new Upsert<>("k1", "acme"), state, out::add);
        leftJoin().onEntity(entity(new Upsert<>("e1", "dev")), state, out::add);

        assertThat(out).containsExactly(new Upsert<>("e1", "dev@acme"));
    }

    @Test
    void storedEntityDeleteEmitsDeleteWithStoredEntity() throws Exception {
        leftJoin().onDimension(new Upsert<>("k1", "acme"), state, out::add);
        leftJoin().onEntity(entity(new Upsert<>("e1", "dev")), state, out::add);
        leftJoin().onEntity(entity(new Delete<>("e1", "dev")), state, out::add);

        assertThat(out).last().isEqualTo(new Delete<>("e1", "dev@acme"));
        assertThat(state.entities()).isEmpty();
    }

    @Test
    void unknownEntityDeleteEmitsNothing() throws Exception {
        leftJoin().onEntity(entity(new Delete<>("e9", "ghost")), state, out::add);

        assertThat(out).isEmpty();
    }

    @Test
    void dimensionUpsertReEmitsEveryEntity() throws Exception {
        leftJoin().onEntity(entity(new Upsert<>("e2", "qa")), state, out::add);
        leftJoin().onEntity(entity(new Upsert<>("e1", "dev")), state, out::add);
        out.clear();
        leftJoin().onDimension(new Upsert<>("k1", "acme"), state, out::add);

        assertThat(out).containsExactly(new Upsert<>("e1", "dev@acme"), new Upsert<>("e2", "qa@acme"));
    }

    @Test
    void dimensionDeleteReEmitsEntitiesWithNullAndKeepsThem() throws Exception {
        leftJoin().onDimension(new Upsert<>("k1", "acme"), state, out::add);
        leftJoin().onEntity(entity(new Upsert<>("e1", "dev")), state, out::add);
        out.clear();
        leftJoin().onDimension(new Delete<>("k1", "acme"), state, out::add);

        assertThat(out).containsExactly(new Upsert<>("e1", "dev@null"));
        assertThat(state.entities()).containsKey("e1");
    }

    @Test
    void requireMatchHoldsEntityUntilDimensionArrives() throws Exception {
        innerJoin().onEntity(entity(new Upsert<>("e1", "dev")), state, out::add);
        assertThat(out).isEmpty();

        innerJoin().onDimension(new Upsert<>("k1", "acme"), state, out::add);
        assertThat(out).containsExactly(new Upsert<>("e1", "dev@acme"));
    }

    @Test
    void requireMatchDeletesEntitiesWhenDimensionDeleted() throws Exception {
        innerJoin().onDimension(new Upsert<>("k1", "acme"), state, out::add);
        innerJoin().onEntity(entity(new Upsert<>("e1", "dev")), state, out::add);
        out.clear();
        innerJoin().onDimension(new Delete<>("k1", "acme"), state, out::add);

        assertThat(out).containsExactly(new Delete<>("e1", "dev@acme"));
        assertThat(state.entities()).containsKey("e1");
    }

    @Test
    void requireMatchEntityDeleteWithoutDimensionEmitsNothing() throws Exception {
        innerJoin().onEntity(entity(new Upsert<>("e1", "dev")), state, out::add);
        innerJoin().onEntity(entity(new Delete<>("e1", "dev")), state, out::add);

        assertThat(out).isEmpty();
        assertThat(state.entities()).isEmpty();
    }

    @Test
    void relocatedEntityIsRemovedSilently() throws Exception {
        leftJoin().onDimension(new Upsert<>("k1", "acme"), state, out::add);
        leftJoin().onEntity(entity(new Upsert<>("e1", "dev")), state, out::add);
        out.clear();
        leftJoin().onEntity(new LookupEntity<>(new Delete<>("e1", "dev"), true), state, out::add);

        assertThat(out).isEmpty();
        assertThat(state.entities()).isEmpty();
    }

    @Test
    void enricherFailurePropagates() {
        LookupRules<String, String, String> failing = new LookupRules<>((entity, dimension) -> {
            throw new IllegalStateException("broken enricher");
        }, false);

        assertThatThrownBy(() -> failing.onEntity(entity(new Upsert<>("e1", "dev")), state, out::add))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("broken enricher");
    }

    private static LookupEntity<String> entity(Change<String> change) {
        return new LookupEntity<>(change, false);
    }

    private static LookupRules<String, String, String> leftJoin() {
        return new LookupRules<>((entity, dimension) -> entity + "@" + dimension, false);
    }

    private static LookupRules<String, String, String> innerJoin() {
        return new LookupRules<>((entity, dimension) -> entity + "@" + dimension, true);
    }
}
```

`src/test/java/io/github/mannkostir/projections/LookupEntitySerializerTest.java`:

```java
package io.github.mannkostir.projections;

import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;

class LookupEntitySerializerTest extends SerializerTestBase<LookupEntity<String>> {
    @Override
    protected TypeSerializer<LookupEntity<String>> createSerializer() {
        return new LookupEntitySerializer<>(new ChangeSerializer<>(StringSerializer.INSTANCE));
    }

    @Override
    protected int getLength() {
        return -1;
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    protected Class<LookupEntity<String>> getTypeClass() {
        return (Class) LookupEntity.class;
    }

    @Override
    protected boolean allowNullInstances(TypeSerializer<LookupEntity<String>> serializer) {
        return true;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected LookupEntity<String>[] getTestData() {
        return new LookupEntity[] {
                new LookupEntity<>(new Upsert<>("e1", "dev"), false),
                new LookupEntity<>(new Delete<>("e1", "dev"), true),
                new LookupEntity<>(new Delete<>("e2", "qa"), false)
        };
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `mvn -q -s /private/tmp/claude-501/-Users-mannkostir-Documents-flink-use-cases-framework/3f51feaa-f321-4669-aad0-9b002a5b11e0/scratchpad/settings.xml test -Dtest=LookupRulesTest,LookupEntitySerializerTest`
Expected: BUILD FAILURE with compilation errors: cannot find symbol `LookupRules` / `LookupEntity` / `LookupEntitySerializer`.

- [ ] **Step 3: Write the implementation**

`src/main/java/io/github/mannkostir/projections/Enricher.java`:

```java
package io.github.mannkostir.projections;

import java.io.Serializable;

public interface Enricher<E, D, O> extends Serializable {
    O enrich(E entity, D dimension) throws Exception;
}
```

`src/main/java/io/github/mannkostir/projections/LookupEntity.java`:

```java
package io.github.mannkostir.projections;

record LookupEntity<E>(Change<E> change, boolean relocation) {
}
```

`src/main/java/io/github/mannkostir/projections/LookupState.java`:

```java
package io.github.mannkostir.projections;

import java.util.SortedMap;

interface LookupState<E, D> {
    D dimension() throws Exception;

    void putDimension(D dimension) throws Exception;

    void clearDimension() throws Exception;

    E entity(String entityId) throws Exception;

    void putEntity(String entityId, E entity) throws Exception;

    void removeEntity(String entityId) throws Exception;

    SortedMap<String, E> entities() throws Exception;
}
```

`src/main/java/io/github/mannkostir/projections/LookupRules.java`:

```java
package io.github.mannkostir.projections;

import java.io.Serializable;
import java.util.Map;
import java.util.function.Consumer;

final class LookupRules<E, D, O> implements Serializable {
    private final Enricher<E, D, O> enricher;
    private final boolean requireMatch;

    LookupRules(Enricher<E, D, O> enricher, boolean requireMatch) {
        this.enricher = enricher;
        this.requireMatch = requireMatch;
    }

    void onEntity(LookupEntity<E> entity, LookupState<E, D> state, Consumer<Change<O>> out) throws Exception {
        Change<E> change = entity.change();
        if (entity.relocation()) {
            state.removeEntity(change.id());
            return;
        }
        if (change instanceof Upsert<E> upsert) {
            state.putEntity(upsert.id(), upsert.value());
            emitIfMatchAllows(new Upsert<>(upsert.id(), enricher.enrich(upsert.value(), state.dimension())), state, out);
            return;
        }
        E stored = state.entity(change.id());
        if (stored != null) {
            state.removeEntity(change.id());
            emitIfMatchAllows(new Delete<>(change.id(), enricher.enrich(stored, state.dimension())), state, out);
        }
    }

    void onDimension(Change<D> change, LookupState<E, D> state, Consumer<Change<O>> out) throws Exception {
        if (change instanceof Upsert<D> upsert) {
            state.putDimension(upsert.value());
            reEmitAll(upsert.value(), state, out);
            return;
        }
        D previous = state.dimension();
        state.clearDimension();
        if (requireMatch) {
            deleteAllMatchedBy(previous, state, out);
        } else {
            reEmitAll(null, state, out);
        }
    }

    private void emitIfMatchAllows(Change<O> change, LookupState<E, D> state, Consumer<Change<O>> out) throws Exception {
        if (!requireMatch || state.dimension() != null) {
            out.accept(change);
        }
    }

    private void reEmitAll(D dimension, LookupState<E, D> state, Consumer<Change<O>> out) throws Exception {
        for (Map.Entry<String, E> entity : state.entities().entrySet()) {
            out.accept(new Upsert<>(entity.getKey(), enricher.enrich(entity.getValue(), dimension)));
        }
    }

    private void deleteAllMatchedBy(D previous, LookupState<E, D> state, Consumer<Change<O>> out) throws Exception {
        if (previous == null) {
            return;
        }
        for (Map.Entry<String, E> entity : state.entities().entrySet()) {
            out.accept(new Delete<>(entity.getKey(), enricher.enrich(entity.getValue(), previous)));
        }
    }
}
```

`src/main/java/io/github/mannkostir/projections/LookupEntitySerializer.java`:

```java
package io.github.mannkostir.projections;

import java.io.IOException;

import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

final class LookupEntitySerializer<E> extends TypeSerializer<LookupEntity<E>> {
    private final TypeSerializer<Change<E>> changeSerializer;

    LookupEntitySerializer(TypeSerializer<Change<E>> changeSerializer) {
        this.changeSerializer = changeSerializer;
    }

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TypeSerializer<LookupEntity<E>> duplicate() {
        TypeSerializer<Change<E>> duplicated = changeSerializer.duplicate();
        return duplicated == changeSerializer ? this : new LookupEntitySerializer<>(duplicated);
    }

    @Override
    public LookupEntity<E> createInstance() {
        return null;
    }

    @Override
    public LookupEntity<E> copy(LookupEntity<E> from) {
        return new LookupEntity<>(changeSerializer.copy(from.change()), from.relocation());
    }

    @Override
    public LookupEntity<E> copy(LookupEntity<E> from, LookupEntity<E> reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(LookupEntity<E> entity, DataOutputView target) throws IOException {
        target.writeBoolean(entity.relocation());
        changeSerializer.serialize(entity.change(), target);
    }

    @Override
    public LookupEntity<E> deserialize(DataInputView source) throws IOException {
        boolean relocation = source.readBoolean();
        return new LookupEntity<>(changeSerializer.deserialize(source), relocation);
    }

    @Override
    public LookupEntity<E> deserialize(LookupEntity<E> reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        target.writeBoolean(source.readBoolean());
        changeSerializer.copy(source, target);
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof LookupEntitySerializer<?> that && changeSerializer.equals(that.changeSerializer);
    }

    @Override
    public int hashCode() {
        return changeSerializer.hashCode();
    }

    @Override
    public TypeSerializerSnapshot<LookupEntity<E>> snapshotConfiguration() {
        return new Snapshot<>(this);
    }

    public static final class Snapshot<E> extends CompositeTypeSerializerSnapshot<LookupEntity<E>, LookupEntitySerializer<E>> {
        private static final int VERSION = 1;

        public Snapshot() {
        }

        Snapshot(LookupEntitySerializer<E> serializer) {
            super(serializer);
        }

        @Override
        protected int getCurrentOuterSnapshotVersion() {
            return VERSION;
        }

        @Override
        protected TypeSerializer<?>[] getNestedSerializers(LookupEntitySerializer<E> outerSerializer) {
            return new TypeSerializer<?>[] {outerSerializer.changeSerializer};
        }

        @Override
        @SuppressWarnings("unchecked")
        protected LookupEntitySerializer<E> createOuterSerializerWithNestedSerializers(TypeSerializer<?>[] nestedSerializers) {
            return new LookupEntitySerializer<>((TypeSerializer<Change<E>>) nestedSerializers[0]);
        }
    }
}
```

`src/main/java/io/github/mannkostir/projections/LookupEntityTypeInfo.java`:

```java
package io.github.mannkostir.projections;

import org.apache.flink.api.common.serialization.SerializerConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

final class LookupEntityTypeInfo<E> extends TypeInformation<LookupEntity<E>> {
    private final TypeInformation<E> entityType;

    LookupEntityTypeInfo(TypeInformation<E> entityType) {
        this.entityType = entityType;
    }

    @Override
    public boolean isBasicType() {
        return false;
    }

    @Override
    public boolean isTupleType() {
        return false;
    }

    @Override
    public int getArity() {
        return 1;
    }

    @Override
    public int getTotalFields() {
        return 1;
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public Class<LookupEntity<E>> getTypeClass() {
        return (Class) LookupEntity.class;
    }

    @Override
    public boolean isKeyType() {
        return false;
    }

    @Override
    public TypeSerializer<LookupEntity<E>> createSerializer(SerializerConfig config) {
        return new LookupEntitySerializer<>(new ChangeSerializer<>(entityType.createSerializer(config)));
    }

    @Override
    public String toString() {
        return "LookupEntity<" + entityType + ">";
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof LookupEntityTypeInfo<?> that && that.canEqual(this) && entityType.equals(that.entityType);
    }

    @Override
    public int hashCode() {
        return entityType.hashCode();
    }

    @Override
    public boolean canEqual(Object other) {
        return other instanceof LookupEntityTypeInfo;
    }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `mvn -q -s /private/tmp/claude-501/-Users-mannkostir-Documents-flink-use-cases-framework/3f51feaa-f321-4669-aad0-9b002a5b11e0/scratchpad/settings.xml test -Dtest=LookupRulesTest,LookupEntitySerializerTest`
Expected: exit 0, no test failures.

- [ ] **Step 5: Run the whole suite**

Run: `mvn -q -s /private/tmp/claude-501/-Users-mannkostir-Documents-flink-use-cases-framework/3f51feaa-f321-4669-aad0-9b002a5b11e0/scratchpad/settings.xml clean verify`
Expected: exit 0.

- [ ] **Step 6: Commit**

```bash
git add src/main/java/io/github/mannkostir/projections/Enricher.java src/main/java/io/github/mannkostir/projections/LookupEntity.java src/main/java/io/github/mannkostir/projections/LookupState.java src/main/java/io/github/mannkostir/projections/LookupRules.java src/main/java/io/github/mannkostir/projections/LookupEntitySerializer.java src/main/java/io/github/mannkostir/projections/LookupEntityTypeInfo.java src/test/java/io/github/mannkostir/projections/InMemoryLookupState.java src/test/java/io/github/mannkostir/projections/LookupRulesTest.java src/test/java/io/github/mannkostir/projections/LookupEntitySerializerTest.java
git commit -m "add lookup rules"
```

---

### Task 12: Lookup operator and builder

**Files:**
- Create: `src/main/java/io/github/mannkostir/projections/FlinkLookupState.java`
- Create: `src/main/java/io/github/mannkostir/projections/LookupFunction.java`
- Create: `src/main/java/io/github/mannkostir/projections/LookupEntityKey.java`
- Create: `src/main/java/io/github/mannkostir/projections/LookupEntityTagger.java`
- Create: `src/main/java/io/github/mannkostir/projections/Lookup.java`
- Test: `src/test/java/io/github/mannkostir/projections/LookupFunctionTest.java`
- Test: `src/test/java/io/github/mannkostir/projections/LookupTest.java`
- Test: `src/test/java/io/github/mannkostir/projections/KeySelectorsTest.java`

**Interfaces:**
- Consumes: `LookupRules`, `LookupEntity`, `LookupEntityTypeInfo`, `Enricher` (Task 11); `RoutingFunction`, `ChangeTagger`, `Routed`, `StateTtl` (Task 5); `LookupOptions` (Task 6); `SlotParentKey`, `ChangeId` (Task 9); `NestTest.changes`, `NestTest.uidsUpstreamOf` (Task 10); `ChangesTest.environment()` (Task 4).
- Produces: public `Lookup<E>`: `of(String name, DataStream<Change<E>>, KeySelector<E,String> lookupKey, TypeInformation<E>)`, `withOptions(LookupOptions)`, `from(DataStream<Change<D>>, TypeInformation<D>)` returning `Lookup.WithDimension<E,D>` whose `enrich(Enricher<E,D,O>, TypeInformation<O>)` returns `DataStream<Change<O>>`; package-private `LookupFunction`, `FlinkLookupState`, `LookupEntityKey<E>(String name, KeySelector<E,String>)`, `LookupEntityTagger<E>`.

- [ ] **Step 1: Write the failing tests**

`src/test/java/io/github/mannkostir/projections/LookupFunctionTest.java`:

```java
package io.github.mannkostir.projections;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

class LookupFunctionTest {
    @Test
    void enrichesEntityWithDimension() throws Exception {
        try (var harness = harness(LookupOptions.defaults())) {
            harness.open();
            harness.processElement2(new Upsert<>("k1", "acme"), 1L);
            harness.processElement1(new LookupEntity<>(new Upsert<>("e1", "dev#k1"), false), 2L);

            assertThat(harness.extractOutputValues()).containsExactly(new Upsert<>("e1", "dev#k1@acme"));
        }
    }

    @Test
    void restoresStateFromSavepoint() throws Exception {
        OperatorSubtaskState snapshot;
        try (var harness = harness(LookupOptions.defaults())) {
            harness.open();
            harness.processElement1(new LookupEntity<>(new Upsert<>("e1", "dev#k1"), false), 1L);
            snapshot = harness.snapshot(1L, 1L);
        }

        try (var restored = harness(LookupOptions.defaults())) {
            restored.initializeState(snapshot);
            restored.open();
            restored.processElement2(new Upsert<>("k1", "acme"), 2L);

            assertThat(restored.extractOutputValues()).containsExactly(new Upsert<>("e1", "dev#k1@acme"));
        }
    }

    @Test
    void registersStateUnderContractNames() throws Exception {
        try (var harness = harness(LookupOptions.defaults())) {
            harness.open();
            harness.processElement2(new Upsert<>("k1", "acme"), 1L);
            harness.processElement1(new LookupEntity<>(new Upsert<>("e1", "dev#k1"), false), 2L);

            var backend = harness.getOperator().<String>getKeyedStateBackend();
            assertThat(backend.getKeys("company.entities", VoidNamespace.INSTANCE)).containsExactly("k1");
            assertThat(backend.getKeys("company.dimension", VoidNamespace.INSTANCE)).containsExactly("k1");
        }
    }

    @Test
    void dimensionExpiresAfterTtl() throws Exception {
        LookupOptions options = LookupOptions.builder().stateTtl(Duration.ofMillis(100)).build();
        try (var harness = harness(options)) {
            harness.open();
            harness.setStateTtlProcessingTime(0L);
            harness.processElement2(new Upsert<>("k1", "acme"), 1L);
            harness.setStateTtlProcessingTime(200L);
            harness.processElement1(new LookupEntity<>(new Upsert<>("e1", "dev#k1"), false), 2L);

            assertThat(harness.extractOutputValues()).containsExactly(new Upsert<>("e1", "dev#k1@null"));
        }
    }

    private static KeyedTwoInputStreamOperatorTestHarness<String, LookupEntity<String>, Change<String>, Change<String>> harness(
            LookupOptions options) throws Exception {
        LookupFunction<String, String, String> function = new LookupFunction<>(
                "company", Types.STRING, Types.STRING, (entity, dimension) -> entity + "@" + dimension, options);
        var harness = new KeyedTwoInputStreamOperatorTestHarness<String, LookupEntity<String>, Change<String>, Change<String>>(
                new KeyedCoProcessOperator<>(function),
                new LookupEntityKey<String>("company", value -> value.split("#")[1]),
                new ChangeId<>(),
                Types.STRING);
        harness.setup(new ChangeSerializer<>(StringSerializer.INSTANCE));
        return harness;
    }
}
```

`src/test/java/io/github/mannkostir/projections/LookupTest.java`:

```java
package io.github.mannkostir.projections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class LookupTest {
    @RegisterExtension
    static final MiniClusterExtension CLUSTER = new MiniClusterExtension();

    @Test
    void enrichesEntitiesWithDimension() throws Exception {
        StreamExecutionEnvironment env = ChangesTest.environment();
        DataStream<Change<String>> enriched = Lookup.of("company", NestTest.changes(env, new Upsert<>("e1", "dev@k1")), LookupTest::companyOf, Types.STRING)
                .withOptions(LookupOptions.builder().requireMatch(true).build())
                .from(NestTest.changes(env, new Upsert<>("k1", "acme")), Types.STRING)
                .enrich((entity, company) -> entity + "=" + company, Types.STRING);

        assertThat(enriched.executeAndCollect(10)).containsExactly(new Upsert<>("e1", "dev@k1=acme"));
    }

    @Test
    void namesOperatorsAfterLookup() {
        StreamExecutionEnvironment env = ChangesTest.environment();
        DataStream<Change<String>> enriched = Lookup.of("company", NestTest.changes(env, new Upsert<>("e1", "dev@k1")), LookupTest::companyOf, Types.STRING)
                .from(NestTest.changes(env, new Upsert<>("k1", "acme")), Types.STRING)
                .enrich((entity, company) -> entity, Types.STRING);

        assertThat(enriched.getTransformation().getUid()).isEqualTo("lookup_company");
        assertThat(NestTest.uidsUpstreamOf(enriched.getTransformation())).contains("lookup_company_route");
    }

    @Test
    void rejectsInvalidName() {
        StreamExecutionEnvironment env = ChangesTest.environment();

        assertThatThrownBy(() -> Lookup.of("company_name", NestTest.changes(env, new Upsert<>("e1", "dev@k1")), LookupTest::companyOf, Types.STRING))
                .isInstanceOf(ProjectionConfigurationException.class)
                .hasMessageContaining("'company_name'");
    }

    static String companyOf(String entity) {
        return entity.split("@")[1];
    }
}
```

`src/test/java/io/github/mannkostir/projections/KeySelectorsTest.java`:

```java
package io.github.mannkostir.projections;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.apache.flink.api.java.functions.KeySelector;
import org.junit.jupiter.api.Test;

class KeySelectorsTest {
    @Test
    void nullParentKeyFailsNamingLevelSlotAndChild() {
        KeySelector<Object, String> noParent = value -> null;
        SlotParentKey key = new SlotParentKey("candidate", List.of("skills"), List.of(noParent));

        assertThatThrownBy(() -> key.getKey(new SlotChange(0, new Upsert<>("s1", "java"))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("'candidate'")
                .hasMessageContaining("'skills'")
                .hasMessageContaining("'s1'");
    }

    @Test
    void nullLookupKeyFailsNamingLookupAndEntity() {
        LookupEntityKey<String> key = new LookupEntityKey<>("company", value -> null);

        assertThatThrownBy(() -> key.getKey(new LookupEntity<>(new Upsert<>("e1", "dev"), false)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("'company'")
                .hasMessageContaining("'e1'");
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `mvn -q -s /private/tmp/claude-501/-Users-mannkostir-Documents-flink-use-cases-framework/3f51feaa-f321-4669-aad0-9b002a5b11e0/scratchpad/settings.xml test -Dtest=LookupFunctionTest,LookupTest,KeySelectorsTest`
Expected: BUILD FAILURE with compilation errors: cannot find symbol `LookupFunction` / `LookupEntityKey` / `Lookup`.

- [ ] **Step 3: Write the implementation**

`src/main/java/io/github/mannkostir/projections/FlinkLookupState.java`:

```java
package io.github.mannkostir.projections;

import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.ValueState;

final class FlinkLookupState<E, D> implements LookupState<E, D> {
    private final MapState<String, E> entities;
    private final ValueState<D> dimension;

    FlinkLookupState(MapState<String, E> entities, ValueState<D> dimension) {
        this.entities = entities;
        this.dimension = dimension;
    }

    @Override
    public D dimension() throws Exception {
        return dimension.value();
    }

    @Override
    public void putDimension(D value) throws Exception {
        dimension.update(value);
    }

    @Override
    public void clearDimension() {
        dimension.clear();
    }

    @Override
    public E entity(String entityId) throws Exception {
        return entities.get(entityId);
    }

    @Override
    public void putEntity(String entityId, E entity) throws Exception {
        entities.put(entityId, entity);
    }

    @Override
    public void removeEntity(String entityId) throws Exception {
        entities.remove(entityId);
    }

    @Override
    public SortedMap<String, E> entities() throws Exception {
        SortedMap<String, E> sorted = new TreeMap<>();
        for (Map.Entry<String, E> entry : entities.entries()) {
            sorted.put(entry.getKey(), entry.getValue());
        }
        return sorted;
    }
}
```

`src/main/java/io/github/mannkostir/projections/LookupFunction.java`:

```java
package io.github.mannkostir.projections;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

final class LookupFunction<E, D, O> extends KeyedCoProcessFunction<String, LookupEntity<E>, Change<D>, Change<O>> {
    private final String name;
    private final TypeInformation<E> entityType;
    private final TypeInformation<D> dimensionType;
    private final LookupOptions options;
    private final LookupRules<E, D, O> rules;
    private transient LookupState<E, D> state;

    LookupFunction(
            String name,
            TypeInformation<E> entityType,
            TypeInformation<D> dimensionType,
            Enricher<E, D, O> enricher,
            LookupOptions options) {
        this.name = name;
        this.entityType = entityType;
        this.dimensionType = dimensionType;
        this.options = options;
        this.rules = new LookupRules<>(enricher, options.requireMatch());
    }

    @Override
    public void open(OpenContext openContext) {
        state = new FlinkLookupState<>(
                getRuntimeContext().getMapState(StateTtl.applyTo(
                        new MapStateDescriptor<>(ContractNames.lookupEntitiesState(name), Types.STRING, entityType),
                        options.stateTtl())),
                getRuntimeContext().getState(StateTtl.applyTo(
                        new ValueStateDescriptor<>(ContractNames.lookupDimensionState(name), dimensionType),
                        options.stateTtl())));
    }

    @Override
    public void processElement1(LookupEntity<E> entity, Context context, Collector<Change<O>> out) throws Exception {
        rules.onEntity(entity, state, out::collect);
    }

    @Override
    public void processElement2(Change<D> change, Context context, Collector<Change<O>> out) throws Exception {
        rules.onDimension(change, state, out::collect);
    }
}
```

`src/main/java/io/github/mannkostir/projections/LookupEntityKey.java`:

```java
package io.github.mannkostir.projections;

import org.apache.flink.api.java.functions.KeySelector;

final class LookupEntityKey<E> implements KeySelector<LookupEntity<E>, String> {
    private final String name;
    private final KeySelector<E, String> lookupKey;

    LookupEntityKey(String name, KeySelector<E, String> lookupKey) {
        this.name = name;
        this.lookupKey = lookupKey;
    }

    @Override
    public String getKey(LookupEntity<E> entity) throws Exception {
        String key = lookupKey.getKey(entity.change().value());
        if (key == null) {
            throw new IllegalStateException("Lookup '" + name + "': lookup key selector returned null for entity id '"
                    + entity.change().id() + "'");
        }
        return key;
    }
}
```

`src/main/java/io/github/mannkostir/projections/LookupEntityTagger.java`:

```java
package io.github.mannkostir.projections;

final class LookupEntityTagger<E> implements ChangeTagger<E, LookupEntity<E>> {
    @Override
    public LookupEntity<E> tag(Routed<E> routed) {
        return new LookupEntity<>(routed.change(), routed.relocation());
    }
}
```

`src/main/java/io/github/mannkostir/projections/Lookup.java`:

```java
package io.github.mannkostir.projections;

import java.util.Objects;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;

public final class Lookup<E> {
    private final String name;
    private final DataStream<Change<E>> entities;
    private final KeySelector<E, String> lookupKey;
    private final TypeInformation<E> entityType;
    private LookupOptions options = LookupOptions.defaults();

    private Lookup(String name, DataStream<Change<E>> entities, KeySelector<E, String> lookupKey, TypeInformation<E> entityType) {
        this.name = name;
        this.entities = entities;
        this.lookupKey = lookupKey;
        this.entityType = entityType;
    }

    public static <E> Lookup<E> of(
            String name, DataStream<Change<E>> entities, KeySelector<E, String> lookupKey, TypeInformation<E> type) {
        return new Lookup<>(Names.requireValid(name, "Lookup name"), entities, lookupKey, type);
    }

    public Lookup<E> withOptions(LookupOptions options) {
        this.options = Objects.requireNonNull(options, "options");
        return this;
    }

    public <D> WithDimension<E, D> from(DataStream<Change<D>> dimensions, TypeInformation<D> type) {
        return new WithDimension<>(this, dimensions, type);
    }

    public static final class WithDimension<E, D> {
        private final Lookup<E> lookup;
        private final DataStream<Change<D>> dimensions;
        private final TypeInformation<D> dimensionType;

        private WithDimension(Lookup<E> lookup, DataStream<Change<D>> dimensions, TypeInformation<D> dimensionType) {
            this.lookup = lookup;
            this.dimensions = dimensions;
            this.dimensionType = dimensionType;
        }

        public <O> DataStream<Change<O>> enrich(Enricher<E, D, O> enricher, TypeInformation<O> type) {
            String uid = ContractNames.lookupUid(lookup.name);
            return routedEntities()
                    .connect(dimensions)
                    .keyBy(new LookupEntityKey<>(lookup.name, lookup.lookupKey), new ChangeId<>(), Types.STRING)
                    .process(new LookupFunction<>(lookup.name, lookup.entityType, dimensionType, enricher, lookup.options),
                            Changes.typeInfo(type))
                    .uid(uid)
                    .name(uid);
        }

        private DataStream<LookupEntity<E>> routedEntities() {
            String uid = ContractNames.lookupRouteUid(lookup.name);
            RoutingFunction<E, LookupEntity<E>> routing = new RoutingFunction<>(
                    ContractNames.lookupRouteState(lookup.name),
                    lookup.lookupKey,
                    lookup.entityType,
                    lookup.options.stateTtl(),
                    new LookupEntityTagger<>());
            return lookup.entities.keyBy(new ChangeId<>(), Types.STRING)
                    .process(routing, new LookupEntityTypeInfo<>(lookup.entityType))
                    .uid(uid)
                    .name(uid);
        }
    }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `mvn -q -s /private/tmp/claude-501/-Users-mannkostir-Documents-flink-use-cases-framework/3f51feaa-f321-4669-aad0-9b002a5b11e0/scratchpad/settings.xml test -Dtest=LookupFunctionTest,LookupTest,KeySelectorsTest`
Expected: exit 0, no test failures.

- [ ] **Step 5: Run the whole suite**

Run: `mvn -q -s /private/tmp/claude-501/-Users-mannkostir-Documents-flink-use-cases-framework/3f51feaa-f321-4669-aad0-9b002a5b11e0/scratchpad/settings.xml clean verify`
Expected: exit 0.

- [ ] **Step 6: Commit**

```bash
git add src/main/java/io/github/mannkostir/projections/FlinkLookupState.java src/main/java/io/github/mannkostir/projections/LookupFunction.java src/main/java/io/github/mannkostir/projections/LookupEntityKey.java src/main/java/io/github/mannkostir/projections/LookupEntityTagger.java src/main/java/io/github/mannkostir/projections/Lookup.java src/test/java/io/github/mannkostir/projections/LookupFunctionTest.java src/test/java/io/github/mannkostir/projections/LookupTest.java src/test/java/io/github/mannkostir/projections/KeySelectorsTest.java
git commit -m "add lookup operator"
```

---

### Task 13: Resume-model pipeline test

**Files:**
- Test: `src/test/java/io/github/mannkostir/projections/ResumeProjectionPipelineTest.java`

**Interfaces:**
- Consumes: `Nest`, `ChildSlot`, `Lookup`, `Changes`, `Change`, `Upsert`, `Delete` (Tasks 2-12); `ChangesTest.environment()` (Task 4).
- Produces: nothing new; proves the public API end to end.

- [ ] **Step 1: Write the failing test**

`src/test/java/io/github/mannkostir/projections/ResumeProjectionPipelineTest.java`:

```java
package io.github.mannkostir.projections;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class ResumeProjectionPipelineTest {
    @RegisterExtension
    static final MiniClusterExtension CLUSTER = new MiniClusterExtension();

    public record Candidate(String id, String name) {
    }

    public record Skill(String id, String candidateId, String name) {
    }

    public record Company(String id, String name) {
    }

    public record Experience(String id, String candidateId, String title, String companyId, String companyName) {
        Experience withCompanyName(String name) {
            return new Experience(id, candidateId, title, companyId, name);
        }
    }

    public record Project(String id, String experienceId, String name) {
    }

    public record ExperienceDoc(String id, String candidateId, String title, String companyName, List<String> projects) {
    }

    public record CandidateDoc(String id, String name, List<ExperienceDoc> experiences, List<String> skills) {
    }

    private static final TypeInformation<Candidate> CANDIDATE = TypeInformation.of(Candidate.class);
    private static final TypeInformation<Skill> SKILL = TypeInformation.of(Skill.class);
    private static final TypeInformation<Company> COMPANY = TypeInformation.of(Company.class);
    private static final TypeInformation<Experience> EXPERIENCE = TypeInformation.of(Experience.class);
    private static final TypeInformation<Project> PROJECT = TypeInformation.of(Project.class);
    private static final TypeInformation<ExperienceDoc> EXPERIENCE_DOC = TypeInformation.of(ExperienceDoc.class);
    private static final TypeInformation<CandidateDoc> CANDIDATE_DOC = TypeInformation.of(CandidateDoc.class);

    @Test
    void convergesToOneDocumentPerCandidate() throws Exception {
        StreamExecutionEnvironment env = ChangesTest.environment();

        DataStream<Change<Candidate>> candidates = stream(env, CANDIDATE,
                new Upsert<>("c1", new Candidate("c1", "alice")),
                new Upsert<>("c2", new Candidate("c2", "bob")),
                new Upsert<>("c3", new Candidate("c3", "carol")),
                new Delete<>("c3", new Candidate("c3", "carol")));
        DataStream<Change<Skill>> skills = stream(env, SKILL,
                new Upsert<>("s1", new Skill("s1", "c1", "java")),
                new Upsert<>("s2", new Skill("s2", "c3", "go")));
        DataStream<Change<Company>> companies = stream(env, COMPANY,
                new Upsert<>("k1", new Company("k1", "Acme")),
                new Upsert<>("k2", new Company("k2", "Globex")),
                new Upsert<>("k1", new Company("k1", "Acme Corp")));
        DataStream<Change<Experience>> experiences = stream(env, EXPERIENCE,
                new Upsert<>("e1", new Experience("e1", "c1", "dev", "k1", null)),
                new Upsert<>("e2", new Experience("e2", "c1", "qa", "k2", null)),
                new Upsert<>("e2", new Experience("e2", "c2", "qa", "k2", null)));
        DataStream<Change<Project>> projects = stream(env, PROJECT,
                new Upsert<>("p1", new Project("p1", "e1", "search")),
                new Upsert<>("p2", new Project("p2", "e1", "index")),
                new Delete<>("p2", new Project("p2", "e1", "index")));

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

        Nest<Candidate> candidateLevel = Nest.parent("candidate", candidates, CANDIDATE);
        ChildSlot<ExperienceDoc> experienceSlot = candidateLevel.child("experiences", experienceDocs, ExperienceDoc::candidateId, EXPERIENCE_DOC);
        ChildSlot<Skill> skillSlot = candidateLevel.child("skills", skills, Skill::candidateId, SKILL);
        DataStream<Change<CandidateDoc>> documents = candidateLevel.assemble(
                (candidate, children) -> new CandidateDoc(
                        candidate.id(),
                        candidate.name(),
                        children.get(experienceSlot),
                        children.get(skillSlot).stream().map(Skill::name).toList()),
                CANDIDATE_DOC);

        Map<String, Change<CandidateDoc>> latest = latestById(documents.executeAndCollect(1000));

        assertThat(latest.get("c1")).isEqualTo(new Upsert<>("c1", new CandidateDoc("c1", "alice",
                List.of(new ExperienceDoc("e1", "c1", "dev", "Acme Corp", List.of("search"))),
                List.of("java"))));
        assertThat(latest.get("c2")).isEqualTo(new Upsert<>("c2", new CandidateDoc("c2", "bob",
                List.of(new ExperienceDoc("e2", "c2", "qa", "Globex", List.of())),
                List.of())));
        assertThat(latest.get("c3")).isInstanceOf(Delete.class);
    }

    @SafeVarargs
    private static <T> DataStream<Change<T>> stream(StreamExecutionEnvironment env, TypeInformation<T> type, Change<T>... changes) {
        return env.fromData(List.of(changes), Changes.typeInfo(type));
    }

    private static <T> Map<String, Change<T>> latestById(List<Change<T>> changes) {
        Map<String, Change<T>> latest = new LinkedHashMap<>();
        changes.forEach(change -> latest.put(change.id(), change));
        return latest;
    }
}
```

- [ ] **Step 2: Run the tests to verify they pass**

Run: `mvn -q -s /private/tmp/claude-501/-Users-mannkostir-Documents-flink-use-cases-framework/3f51feaa-f321-4669-aad0-9b002a5b11e0/scratchpad/settings.xml test -Dtest=ResumeProjectionPipelineTest`
Expected: exit 0, no test failures.

- [ ] **Step 3: Run the whole suite**

Run: `mvn -q -s /private/tmp/claude-501/-Users-mannkostir-Documents-flink-use-cases-framework/3f51feaa-f321-4669-aad0-9b002a5b11e0/scratchpad/settings.xml clean verify`
Expected: exit 0.

- [ ] **Step 4: Commit**

```bash
git add src/test/java/io/github/mannkostir/projections/ResumeProjectionPipelineTest.java
git commit -m "add resume projection pipeline test"
```

---

### Task 14: README and CLAUDE.md

**Files:**
- Modify: `README.md`
- Modify: `CLAUDE.md` (local and gitignored, so it gets no commit)

**Interfaces:**
- Consumes: the finished API from Tasks 2-13.
- Produces: user-facing and contributor-facing docs that match the code.

- [ ] **Step 1: Write the failing check**

Run: `grep -c "(planned)\|Job\` scaffolding\|Kafka and Avro" README.md; grep -c "processing/\|Director.java\|EnrichStreamFunction\|No tests at all\|Confluent repo reachable" CLAUDE.md`
Expected: both counts are non-zero. The check passes when both are `0`.

- [ ] **Step 2: Edit README.md**

Make exactly these three replacements:
- `removing the root deletes it (planned).` becomes `removing the root deletes it.`
- `with each TTL set by you (planned).` becomes `with each TTL set by you.`
- ``- **Plain operators** you wire into your own job, with optional `Job` scaffolding for the batteries-included path.`` becomes ``- **Plain operators** you wire into your own job: `Changes`, `Nest` and `Lookup`.``

Replace the last paragraph (under `## Dependencies`) with exactly:

```markdown
Flink is `provided`: your job owns its version. Nothing is shaded or bundled.
```

- [ ] **Step 3: Replace CLAUDE.md's `## Architecture` section**

Replace everything from the line `## Architecture` down to, but not including, the line `## Commands` with exactly:

````markdown
## Architecture

```
src/main/java/io/github/mannkostir/projections/
├── Change, Upsert, Delete, Changes        # public change model and the stream adapter
├── Nest, ChildSlot, Children, Assembler   # public: one tree level, parent + child slots
├── Lookup, Enricher                       # public: attach dimension data by key
├── NestOptions, ChildOptions, LookupOptions, ProjectionConfigurationException
├── RoutingRules, NestRules, LookupRules   # Flink-free decision logic, unit-tested in isolation
├── LevelState, LookupState                # state interfaces; Flink* implementations back them
├── RoutingFunction, NestFunction, LookupFunction   # thin Flink adapters over the rules
└── *Serializer, *TypeInfo                 # explicit serializers with snapshots; no Kryo
```

Everything lives in one package so internals stay package-private. The public types above are the whole
contract.

### How a projection flows

- **Changes:** user streams become `Change<T>` via `Changes.from`. A `Delete` carries the last known value.
- **Nest:** each level keys the parent and its unioned child slots by parent id, and emits the assembled
  document. Levels chain bottom-up.
- **Routing:** every child slot, and every lookup entity input, first passes a routing step keyed by
  entity id. It remembers the last value, so a change of parent or lookup key sends a relocation to the
  old key. For `Nest` a relocation is a child delete; for `Lookup` it silently removes the entity from the
  old key.

### Behaviour worth knowing before you change it

- **Names are the savepoint contract.** `uid`s and state descriptor names come only from user-given names,
  via `ContractNames`, never from class names. Changing one breaks every user's savepoints, and
  `ContractNamesTest` pins them.
- **Serializer snapshots are contract too.** `ChangeSerializer`, `SlotChangeSerializer` and
  `LookupEntitySerializer` versions must only ever move forward compatibly.
- **Ordering assumption:** changes for one entity arrive in order, which is true for sources partitioned by
  entity id. Documents converge; there is no cross-entity ordering.
- **Known limitation:** an entity's lookup key changing at the same moment as its old dimension can yield
  one stale enrichment until that entity's next change.
````

- [ ] **Step 4: Edit CLAUDE.md's `## Known debt` list**

Delete these bullets entirely:
- "No tests at all"
- "Schema Registry is not yet optional"
- "Local builds need the Confluent repo reachable"
- "Singletons"
- "`KafkaAdmin` auto-creates topics"
- "`Job.getKafkaSourceBuilder` branches"
- "`Director.start()` wraps"
- "`PrimitiveArraySerializer` is an"
- "README promises not yet implemented"

Keep the "No release pipeline" bullet.

- [ ] **Step 5: Run the check**

Run: `grep -c "(planned)\|Job\` scaffolding\|Kafka and Avro" README.md; grep -c "processing/\|Director.java\|EnrichStreamFunction\|No tests at all\|Confluent repo reachable" CLAUDE.md; git status --short CLAUDE.md`
Expected: `0`, then `0`, then no output.

- [ ] **Step 6: Commit the README only**

```bash
git add README.md
git commit -m "update readme for operator api"
```
