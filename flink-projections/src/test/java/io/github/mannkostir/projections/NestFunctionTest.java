package io.github.mannkostir.projections;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OperatorSnapshotUtil;
import org.junit.jupiter.api.Test;

class NestFunctionTest {
    private static final ChildSlot<String> SKILLS = new ChildSlot<>("candidate", "skills", 0);
    private static final NestOptions ORPHAN_TIMEOUT_100_MS =
            NestOptions.builder().orphanTimeout(Duration.ofMillis(100)).build();
    private static final String RELEASE_0_1_0_ORPHAN_TIMERS = resourcePath("nest-orphan-timers-0.1.0.snapshot");

    @Test
    void childUpsertWithoutParentIsStoredSilently() throws Exception {
        try (var harness = harness(NestOptions.defaults(), ChildOptions.defaults())) {
            harness.open();
            harness.processElement2(skill(new Upsert<>("s1", "java@c1")), 1L);

            assertThat(harness.extractOutputValues()).isEmpty();
            assertThat(stateOf(harness, "c1")).isEqualTo(new KeyState(null, Map.of("s1", "java@c1"), null));
        }
    }

    @Test
    void childUpsertWithParentEmitsDocument() throws Exception {
        try (var harness = harness(NestOptions.defaults(), ChildOptions.defaults())) {
            harness.open();
            harness.processElement1(new Upsert<>("c1", "alice"), 1L);
            harness.processElement2(skill(new Upsert<>("s1", "java@c1")), 2L);

            assertThat(harness.extractOutputValues()).last().isEqualTo(new Upsert<>("c1", "alice[java@c1]"));
            assertThat(stateOf(harness, "c1"))
                    .isEqualTo(new KeyState("alice", Map.of("s1", "java@c1"), "alice[java@c1]"));
        }
    }

    @Test
    void storedChildDeleteWithParentEmitsDocumentWithoutIt() throws Exception {
        try (var harness = harness(NestOptions.defaults(), ChildOptions.defaults())) {
            harness.open();
            harness.processElement1(new Upsert<>("c1", "alice"), 1L);
            harness.processElement2(skill(new Upsert<>("s1", "java@c1")), 2L);
            harness.processElement2(skill(new Delete<>("s1", "java@c1")), 3L);

            assertThat(harness.extractOutputValues()).last().isEqualTo(new Upsert<>("c1", "alice[]"));
            assertThat(stateOf(harness, "c1")).isEqualTo(new KeyState("alice", Map.of(), "alice[]"));
        }
    }

    @Test
    void unknownChildDeleteEmitsNothing() throws Exception {
        try (var harness = harness(NestOptions.defaults(), ChildOptions.defaults())) {
            harness.open();
            harness.processElement1(new Upsert<>("c1", "alice"), 1L);
            harness.processElement2(skill(new Delete<>("s9", "rust@c1")), 2L);

            assertThat(harness.extractOutputValues()).containsExactly(new Upsert<>("c1", "alice[]"));
            assertThat(stateOf(harness, "c1")).isEqualTo(new KeyState("alice", Map.of(), "alice[]"));
        }
    }

    @Test
    void storedChildDeleteWithoutParentRemovesSilently() throws Exception {
        try (var harness = harness(NestOptions.defaults(), ChildOptions.defaults())) {
            harness.open();
            harness.processElement2(skill(new Upsert<>("s1", "java@c1")), 1L);
            harness.processElement2(skill(new Delete<>("s1", "java@c1")), 2L);

            assertThat(harness.extractOutputValues()).isEmpty();
            assertThat(stateOf(harness, "c1")).isEqualTo(KeyState.EMPTY);
        }
    }

    @Test
    void parentUpsertEmitsDocumentWithStoredChildren() throws Exception {
        try (var harness = harness(NestOptions.defaults(), ChildOptions.defaults())) {
            harness.open();
            harness.processElement2(skill(new Upsert<>("s1", "java@c1")), 1L);
            harness.processElement1(new Upsert<>("c1", "alice"), 2L);

            assertThat(harness.extractOutputValues()).containsExactly(new Upsert<>("c1", "alice[java@c1]"));
            assertThat(stateOf(harness, "c1"))
                    .isEqualTo(new KeyState("alice", Map.of("s1", "java@c1"), "alice[java@c1]"));
        }
    }

    @Test
    void parentDeleteEmitsDeleteWithLastDocumentAndClearsEverything() throws Exception {
        try (var harness = harness(NestOptions.defaults(), ChildOptions.defaults())) {
            harness.open();
            harness.processElement1(new Upsert<>("c1", "alice"), 1L);
            harness.processElement2(skill(new Upsert<>("s1", "java@c1")), 2L);
            harness.processElement1(new Delete<>("c1", "alice"), 3L);

            assertThat(harness.extractOutputValues()).last().isEqualTo(new Delete<>("c1", "alice[java@c1]"));
            assertThat(stateOf(harness, "c1")).isEqualTo(KeyState.EMPTY);
        }
    }

    @Test
    void parentDeleteWithoutParentClearsChildrenSilently() throws Exception {
        try (var harness = harness(NestOptions.defaults(), ChildOptions.defaults())) {
            harness.open();
            harness.processElement2(skill(new Upsert<>("s1", "java@c1")), 1L);
            harness.processElement1(new Delete<>("c1", "alice"), 2L);

            assertThat(harness.extractOutputValues()).isEmpty();
            assertThat(stateOf(harness, "c1")).isEqualTo(KeyState.EMPTY);
        }
    }

    @Test
    void relocationMovesChildFromOldParentToNewParent() throws Exception {
        try (var harness = harness(NestOptions.defaults(), ChildOptions.defaults())) {
            harness.open();
            harness.processElement1(new Upsert<>("c1", "alice"), 1L);
            harness.processElement1(new Upsert<>("c2", "bob"), 2L);
            harness.processElement2(skill(new Upsert<>("s1", "java@c1")), 3L);
            harness.processElement2(skill(new Delete<>("s1", "java@c1")), 4L);
            harness.processElement2(skill(new Upsert<>("s1", "java@c2")), 5L);

            assertThat(harness.extractOutputValues()).endsWith(
                    new Upsert<>("c1", "alice[]"),
                    new Upsert<>("c2", "bob[java@c2]"));
            assertThat(List.of(stateOf(harness, "c1"), stateOf(harness, "c2"))).containsExactly(
                    new KeyState("alice", Map.of(), "alice[]"),
                    new KeyState("bob", Map.of("s1", "java@c2"), "bob[java@c2]"));
        }
    }

    @Test
    void orphanTimeoutClearsChildrenWhenParentAbsent() throws Exception {
        try (var harness = harness(ORPHAN_TIMEOUT_100_MS, ChildOptions.defaults())) {
            harness.open();
            harness.setProcessingTime(0L);
            harness.processElement2(skill(new Upsert<>("s1", "java@c1")), 1L);
            harness.setProcessingTime(200L);

            assertThat(harness.extractOutputValues()).isEmpty();
            assertThat(stateOf(harness, "c1")).isEqualTo(KeyState.EMPTY);
        }
    }

    @Test
    void orphanTimeoutKeepsChildrenWhenParentArrived() throws Exception {
        try (var harness = harness(ORPHAN_TIMEOUT_100_MS, ChildOptions.defaults())) {
            harness.open();
            harness.setProcessingTime(0L);
            harness.processElement2(skill(new Upsert<>("s1", "java@c1")), 1L);
            harness.processElement1(new Upsert<>("c1", "alice"), 2L);
            harness.setProcessingTime(200L);

            assertThat(harness.extractOutputValues()).containsExactly(new Upsert<>("c1", "alice[java@c1]"));
            assertThat(stateOf(harness, "c1"))
                    .isEqualTo(new KeyState("alice", Map.of("s1", "java@c1"), "alice[java@c1]"));
        }
    }

    @Test
    void repeatedOrphanUpsertsKeepOneTimerPerKey() throws Exception {
        try (var harness = harness(ORPHAN_TIMEOUT_100_MS, ChildOptions.defaults())) {
            harness.open();
            harness.setProcessingTime(30L);
            harness.processElement2(skill(new Upsert<>("s1", "java@c1")), 1L);
            harness.setProcessingTime(140L);
            harness.processElement2(skill(new Upsert<>("s2", "scala@c1")), 2L);
            harness.setProcessingTime(190L);
            harness.processElement2(skill(new Upsert<>("s3", "rust@c1")), 3L);

            assertThat(harness.numProcessingTimeTimers()).isEqualTo(1);
        }
    }

    @Test
    void orphanChildrenSurviveUntilDeadlineAfterLastUpsert() throws Exception {
        try (var harness = harness(ORPHAN_TIMEOUT_100_MS, ChildOptions.defaults())) {
            harness.open();
            harness.setProcessingTime(30L);
            harness.processElement2(skill(new Upsert<>("s1", "java@c1")), 1L);
            harness.setProcessingTime(140L);
            harness.processElement2(skill(new Upsert<>("s2", "scala@c1")), 2L);
            harness.setProcessingTime(317L);

            assertThat(stateOf(harness, "c1"))
                    .isEqualTo(new KeyState(null, Map.of("s1", "java@c1", "s2", "scala@c1"), null));
        }
    }

    @Test
    void orphanChildrenAreClearedAtDeadlineAfterLastUpsert() throws Exception {
        try (var harness = harness(ORPHAN_TIMEOUT_100_MS, ChildOptions.defaults())) {
            harness.open();
            harness.setProcessingTime(30L);
            harness.processElement2(skill(new Upsert<>("s1", "java@c1")), 1L);
            harness.setProcessingTime(140L);
            harness.processElement2(skill(new Upsert<>("s2", "scala@c1")), 2L);
            harness.setProcessingTime(318L);

            assertThat(stateOf(harness, "c1")).isEqualTo(KeyState.EMPTY);
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
    void restoresPendingOrphanTimersFromReleaseSavepoint() throws Exception {
        try (var restored = harness(ORPHAN_TIMEOUT_100_MS, ChildOptions.defaults())) {
            restored.initializeState(OperatorSnapshotUtil.readStateHandle(RELEASE_0_1_0_ORPHAN_TIMERS));
            restored.open();

            assertThat(restored.numProcessingTimeTimers()).isEqualTo(4);
        }
    }

    @Test
    void orphanTimersFromReleaseSavepointClearOnlyOrphanedKeys() throws Exception {
        try (var restored = harness(ORPHAN_TIMEOUT_100_MS, ChildOptions.defaults())) {
            restored.initializeState(OperatorSnapshotUtil.readStateHandle(RELEASE_0_1_0_ORPHAN_TIMERS));
            restored.open();
            restored.setProcessingTime(140L);

            assertThat(List.of(stateOf(restored, "c1"), stateOf(restored, "c2"), stateOf(restored, "c3")))
                    .containsExactly(
                            KeyState.EMPTY,
                            KeyState.EMPTY,
                            new KeyState("carol", Map.of("s5", "kotlin@c3"), "carol[kotlin@c3]"));
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

    private static KeyState stateOf(
            KeyedTwoInputStreamOperatorTestHarness<String, Change<String>, SlotChange, Change<String>> harness,
            String key) throws Exception {
        var backend = harness.getOperator().<String>getKeyedStateBackend();
        backend.setCurrentKey(key);
        ValueState<String> parent = backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE,
                new ValueStateDescriptor<>("candidate.parent", Types.STRING));
        MapState<String, String> skills = backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE,
                new MapStateDescriptor<>("candidate.child.skills", Types.STRING, Types.STRING));
        ValueState<String> lastDoc = backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE,
                new ValueStateDescriptor<>("candidate.last-doc", Types.STRING));
        Map<String, String> storedSkills = new HashMap<>();
        for (Map.Entry<String, String> entry : skills.entries()) {
            storedSkills.put(entry.getKey(), entry.getValue());
        }
        return new KeyState(parent.value(), storedSkills, lastDoc.value());
    }

    private record KeyState(String parent, Map<String, String> skills, String lastDoc) {
        static final KeyState EMPTY = new KeyState(null, Map.of(), null);
    }

    private static String resourcePath(String name) {
        try {
            return Path.of(NestFunctionTest.class.getClassLoader().getResource(name).toURI()).toString();
        } catch (URISyntaxException e) {
            throw new IllegalStateException(e);
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
