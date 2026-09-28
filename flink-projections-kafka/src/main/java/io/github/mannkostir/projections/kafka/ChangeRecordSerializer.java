package io.github.mannkostir.projections.kafka;

import java.nio.charset.StandardCharsets;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import io.github.mannkostir.projections.Change;
import io.github.mannkostir.projections.Delete;

final class ChangeRecordSerializer<T> implements KafkaRecordSerializationSchema<Change<T>> {
    private final String topic;
    private final SerializationSchema<T> format;

    ChangeRecordSerializer(String topic, SerializationSchema<T> format) {
        this.topic = topic;
        this.format = format;
    }

    @Override
    public void open(SerializationSchema.InitializationContext context, KafkaSinkContext sinkContext) throws Exception {
        format.open(context);
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(Change<T> change, KafkaSinkContext context, Long timestamp) {
        byte[] key = change.id().getBytes(StandardCharsets.UTF_8);
        return new ProducerRecord<>(topic, key, valueOf(change));
    }

    private byte[] valueOf(Change<T> change) {
        return change instanceof Delete<T> ? null : format.serialize(change.value());
    }
}
