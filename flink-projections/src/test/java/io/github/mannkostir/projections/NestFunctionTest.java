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
