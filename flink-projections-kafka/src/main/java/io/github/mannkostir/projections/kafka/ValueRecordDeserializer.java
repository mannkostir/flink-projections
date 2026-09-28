package io.github.mannkostir.projections.kafka;

import java.io.IOException;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

final class ValueRecordDeserializer<T> implements KafkaRecordDeserializationSchema<T> {
    private final DeserializationSchema<T> format;
    private final TypeInformation<T> type;

    ValueRecordDeserializer(DeserializationSchema<T> format, TypeInformation<T> type) {
        this.format = format;
        this.type = type;
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        format.open(context);
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<T> out) throws IOException {
        if (record.value() == null) {
            throw new TombstoneNotSupportedException(record.topic(), record.partition(), record.offset());
        }
        format.deserialize(record.value(), out);
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return type;
    }
}
