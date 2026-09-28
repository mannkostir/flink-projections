package io.github.mannkostir.projections.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

class ValueRecordDeserializerTest {
    private final ValueRecordDeserializer<String> deserializer = new ValueRecordDeserializer<>(new SimpleStringSchema(), Types.STRING);

    @Test
    void deserializesRecordValue() throws Exception {
        List<String> out = new ArrayList<>();

        deserializer.deserialize(record("a:live".getBytes(StandardCharsets.UTF_8)), new ListCollector<>(out));

        assertThat(out).containsExactly("a:live");
    }

    @Test
    void rejectsTombstoneNamingItsPosition() {
        List<String> out = new ArrayList<>();

        assertThatThrownBy(() -> deserializer.deserialize(record(null), new ListCollector<>(out)))
                .isInstanceOf(TombstoneNotSupportedException.class)
                .hasMessageContaining("rows-2@7")
                .hasMessageContaining("DeleteDetection.flag");
    }

    @Test
    void skipsRecordsTheFormatDiscards() throws Exception {
        ValueRecordDeserializer<String> discarding = new ValueRecordDeserializer<>(new DiscardingSchema(), Types.STRING);
        List<String> out = new ArrayList<>();

        discarding.deserialize(record("unparseable".getBytes(StandardCharsets.UTF_8)), new ListCollector<>(out));

        assertThat(out).isEmpty();
    }

    @Test
    void producesTheGivenType() {
        assertThat(deserializer.getProducedType()).isEqualTo(Types.STRING);
    }

    private static final class DiscardingSchema extends SimpleStringSchema {
        @Override
        public String deserialize(byte[] message) {
            return null;
        }
    }

    private static ConsumerRecord<byte[], byte[]> record(byte[] value) {
        return new ConsumerRecord<>("rows", 2, 7L, "a".getBytes(StandardCharsets.UTF_8), value);
    }
}
