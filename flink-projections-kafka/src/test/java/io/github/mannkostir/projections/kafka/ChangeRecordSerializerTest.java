package io.github.mannkostir.projections.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

import io.github.mannkostir.projections.Delete;
import io.github.mannkostir.projections.Upsert;

class ChangeRecordSerializerTest {
    private final ChangeRecordSerializer<String> serializer = new ChangeRecordSerializer<>("docs", new SimpleStringSchema());

    @Test
    void writesUpsertAsKeyedValue() {
        ProducerRecord<byte[], byte[]> record = serializer.serialize(new Upsert<>("c1", "alice"), null, null);

        assertThat(record.topic()).isEqualTo("docs");
        assertThat(new String(record.key(), StandardCharsets.UTF_8)).isEqualTo("c1");
        assertThat(new String(record.value(), StandardCharsets.UTF_8)).isEqualTo("alice");
    }

    @Test
    void writesDeleteAsKeyedTombstone() {
        ProducerRecord<byte[], byte[]> record = serializer.serialize(new Delete<>("c1", "alice"), null, null);

        assertThat(record.topic()).isEqualTo("docs");
        assertThat(new String(record.key(), StandardCharsets.UTF_8)).isEqualTo("c1");
        assertThat(record.value()).isNull();
    }

    @Test
    void encodesNonAsciiIdAsUtf8() {
        ProducerRecord<byte[], byte[]> record = serializer.serialize(new Upsert<>("kandidat-ü", "x"), null, null);

        assertThat(record.key()).isEqualTo("kandidat-ü".getBytes(StandardCharsets.UTF_8));
    }
}
