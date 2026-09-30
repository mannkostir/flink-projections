package io.github.mannkostir.projections.elasticsearch;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

class OperationBufferTest {
    private static IndexOperation index(String id, String json) {
        return new IndexOperation(id, json.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void latestOperationForAnIdWins() {
        OperationBuffer buffer = new OperationBuffer();
        buffer.add(index("a", "{\"v\":1}"));
        buffer.add(index("a", "{\"v\":2}"));

        assertThat(buffer.drain()).containsExactly(index("a", "{\"v\":2}"));
    }

    @Test
    void deleteAfterUpsertLeavesOnlyTheDelete() {
        OperationBuffer buffer = new OperationBuffer();
        buffer.add(index("a", "{}"));
        buffer.add(new DeleteOperation("a"));

        assertThat(buffer.drain()).containsExactly(new DeleteOperation("a"));
    }

    @Test
    void upsertAfterDeleteLeavesOnlyTheUpsert() {
        OperationBuffer buffer = new OperationBuffer();
        buffer.add(new DeleteOperation("a"));
        buffer.add(index("a", "{}"));

        assertThat(buffer.drain()).containsExactly(index("a", "{}"));
    }

    @Test
    void drainKeepsFirstSeenOrderOfIds() {
        OperationBuffer buffer = new OperationBuffer();
        buffer.add(index("a", "{}"));
        buffer.add(index("b", "{}"));
        buffer.add(new DeleteOperation("a"));

        assertThat(buffer.drain()).containsExactly(new DeleteOperation("a"), index("b", "{}"));
    }

    @Test
    void countsOneActionPerIdAndTheBytesOfTheLatestOperation() {
        OperationBuffer buffer = new OperationBuffer();
        buffer.add(index("a", "{\"long\":\"value\"}"));
        buffer.add(index("a", "{}"));

        assertThat(buffer.actions()).isEqualTo(1);
        assertThat(buffer.bytes()).isEqualTo(2 + PendingOperation.ACTION_OVERHEAD_BYTES);
    }

    @Test
    void isFullAtTheActionLimit() {
        OperationBuffer buffer = new OperationBuffer();
        buffer.add(new DeleteOperation("a"));
        buffer.add(new DeleteOperation("b"));

        assertThat(buffer.isFull(2, Long.MAX_VALUE)).isTrue();
    }

    @Test
    void isFullAtTheByteLimit() {
        OperationBuffer buffer = new OperationBuffer();
        buffer.add(index("a", "{}"));

        assertThat(buffer.isFull(Integer.MAX_VALUE, 2 + PendingOperation.ACTION_OVERHEAD_BYTES)).isTrue();
    }

    @Test
    void isNotFullBelowBothLimits() {
        OperationBuffer buffer = new OperationBuffer();
        buffer.add(index("a", "{}"));

        assertThat(buffer.isFull(2, 1_000)).isFalse();
    }

    @Test
    void drainEmptiesTheBuffer() {
        OperationBuffer buffer = new OperationBuffer();
        buffer.add(index("a", "{}"));

        buffer.drain();

        assertThat(buffer.isEmpty()).isTrue();
        assertThat(buffer.bytes()).isZero();
    }
}
