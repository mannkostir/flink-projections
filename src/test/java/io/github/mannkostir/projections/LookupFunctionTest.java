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
