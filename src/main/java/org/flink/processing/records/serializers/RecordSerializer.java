package org.flink.processing.records.serializers;

import java.nio.charset.StandardCharsets;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.formats.avro.AvroSerializationSchema;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.flink.processing.records.SinkRecord;

public class RecordSerializer<Payload extends SpecificRecordBase, Sink extends SinkRecord<Payload>>
        implements KafkaRecordSerializationSchema<Sink> {
    private final String topic;
    private final AvroSerializationSchema<Payload> avroSerializationSchema;

    public RecordSerializer (String topic, Class<Payload> outputClass, String schemaRegistryUrl) {
        this.topic = topic;
        this.avroSerializationSchema = ConfluentRegistryAvroSerializationSchema.forSpecific(outputClass,
                                                                                            topic +
                                                                                            "-value",
                                                                                            schemaRegistryUrl
        );
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize (
        Sink record, KafkaSinkContext context, Long timestamp
    ) {
        return new ProducerRecord<>(topic,
                                    null,
                                    record.getKey().getBytes(StandardCharsets.UTF_8),
                                    this.avroSerializationSchema.serialize(record.getPayload())
        );
    }
}
