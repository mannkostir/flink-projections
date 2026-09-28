package io.github.mannkostir.projections;

import java.io.IOException;

import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.types.StringValue;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

final class ChangeSerializer<T> extends TypeSerializer<Change<T>> {
    private static final byte UPSERT = 0;
    private static final byte DELETE = 1;

    private final TypeSerializer<T> valueSerializer;

    ChangeSerializer(TypeSerializer<T> valueSerializer) {
        this.valueSerializer = valueSerializer;
    }

    TypeSerializer<T> valueSerializer() {
        return valueSerializer;
    }

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TypeSerializer<Change<T>> duplicate() {
        TypeSerializer<T> duplicated = valueSerializer.duplicate();
        return duplicated == valueSerializer ? this : new ChangeSerializer<>(duplicated);
    }

    @Override
    public Change<T> createInstance() {
        return null;
    }

    @Override
    public Change<T> copy(Change<T> from) {
        T value = valueSerializer.copy(from.value());
        return from instanceof Delete ? new Delete<>(from.id(), value) : new Upsert<>(from.id(), value);
    }

    @Override
    public Change<T> copy(Change<T> from, Change<T> reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(Change<T> change, DataOutputView target) throws IOException {
        target.writeByte(change instanceof Delete ? DELETE : UPSERT);
        StringValue.writeString(change.id(), target);
        valueSerializer.serialize(change.value(), target);
    }

    @Override
    public Change<T> deserialize(DataInputView source) throws IOException {
        byte kind = source.readByte();
        if (kind != UPSERT && kind != DELETE) {
            throw new IOException("Unknown Change kind " + kind);
        }
        String id = StringValue.readString(source);
        T value = valueSerializer.deserialize(source);
        return kind == DELETE ? new Delete<>(id, value) : new Upsert<>(id, value);
    }

    @Override
    public Change<T> deserialize(Change<T> reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        target.writeByte(source.readByte());
        StringValue.copyString(source, target);
        valueSerializer.copy(source, target);
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof ChangeSerializer<?> that && valueSerializer.equals(that.valueSerializer);
    }

    @Override
    public int hashCode() {
        return valueSerializer.hashCode();
    }

    @Override
    public TypeSerializerSnapshot<Change<T>> snapshotConfiguration() {
        return new Snapshot<>(this);
    }

    public static final class Snapshot<T> extends CompositeTypeSerializerSnapshot<Change<T>, ChangeSerializer<T>> {
        private static final int VERSION = 1;

        public Snapshot() {
        }

        Snapshot(ChangeSerializer<T> serializer) {
            super(serializer);
        }

        @Override
        protected int getCurrentOuterSnapshotVersion() {
            return VERSION;
        }

        @Override
        protected TypeSerializer<?>[] getNestedSerializers(ChangeSerializer<T> outerSerializer) {
            return new TypeSerializer<?>[] {outerSerializer.valueSerializer};
        }

        @Override
        @SuppressWarnings("unchecked")
        protected ChangeSerializer<T> createOuterSerializerWithNestedSerializers(TypeSerializer<?>[] nestedSerializers) {
            return new ChangeSerializer<>((TypeSerializer<T>) nestedSerializers[0]);
        }
    }
}
