package io.github.mannkostir.projections.examples.resume;

import java.io.IOException;

import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.function.SerializableFunction;

final class AvroRecordReader<T> implements DeserializationSchema<T> {
    private final DeserializationSchema<GenericRecord> registry;
    private final SerializableFunction<GenericRecord, T> toValue;
    private final TypeInformation<T> type;

    AvroRecordReader(DeserializationSchema<GenericRecord> registry, SerializableFunction<GenericRecord, T> toValue, TypeInformation<T> type) {
        this.registry = registry;
        this.toValue = toValue;
        this.type = type;
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        registry.open(context);
    }

    @Override
    public T deserialize(byte[] message) throws IOException {
        return toValue.apply(registry.deserialize(message));
    }

    @Override
    public boolean isEndOfStream(T nextElement) {
        return false;
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return type;
    }
}
