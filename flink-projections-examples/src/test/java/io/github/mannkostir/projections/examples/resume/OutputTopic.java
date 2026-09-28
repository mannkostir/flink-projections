package io.github.mannkostir.projections.examples.resume;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

final class OutputTopic implements AutoCloseable {
    private final KafkaConsumer<byte[], byte[]> consumer;
    private final DeserializationSchema<CandidateDoc> reader;
    private final Map<String, Optional<CandidateDoc>> latest = new HashMap<>();

    OutputTopic(String bootstrapServers, String topic, DeserializationSchema<CandidateDoc> reader) {
        this.consumer = new KafkaConsumer<>(consumerProperties(bootstrapServers));
        this.consumer.subscribe(List.of(topic));
        this.reader = reader;
    }

    Map<String, Optional<CandidateDoc>> latestByKey() {
        consumer.poll(Duration.ofMillis(500)).forEach(this::remember);
        return Map.copyOf(latest);
    }

    private void remember(ConsumerRecord<byte[], byte[]> record) {
        latest.put(new String(record.key(), StandardCharsets.UTF_8), Optional.ofNullable(record.value()).map(this::read));
    }

    private CandidateDoc read(byte[] value) {
        try {
            return reader.deserialize(value);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void close() {
        consumer.close();
    }

    private static Properties consumerProperties(String bootstrapServers) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "resume-search-it-reader");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        return properties;
    }
}
