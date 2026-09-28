package io.github.mannkostir.projections.examples.resume;

import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.util.function.SerializableFunction;

final class AvroRecordWriter<T> implements SerializationSchema<T> {
    private final SerializationSchema<GenericRecord> registry;
    private final SerializableFunction<T, GenericRecord> toRecord;

    AvroRecordWriter(SerializationSchema<GenericRecord> registry, SerializableFunction<T, GenericRecord> toRecord) {
        this.registry = registry;
        this.toRecord = toRecord;
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        registry.open(context);
    }

    @Override
    public byte[] serialize(T element) {
        return registry.serialize(toRecord.apply(element));
    }
}
