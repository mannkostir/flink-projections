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
    void forgetsLastValueAfterDelete() throws Exception {
        try (var harness = harness(Optional.empty())) {
            harness.open();
            harness.processElement(new Upsert<>("e1", "e1@a"), 1L);
            harness.processElement(new Delete<>("e1", "e1@a"), 2L);
            harness.processElement(new Upsert<>("e1", "e1@b"), 3L);

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
