package io.github.mannkostir.projections;

import java.io.IOException;

import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

final class LookupEntitySerializer<E> extends TypeSerializer<LookupEntity<E>> {
    private final TypeSerializer<Change<E>> changeSerializer;

    LookupEntitySerializer(TypeSerializer<Change<E>> changeSerializer) {
        this.changeSerializer = changeSerializer;
    }

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TypeSerializer<LookupEntity<E>> duplicate() {
        TypeSerializer<Change<E>> duplicated = changeSerializer.duplicate();
        return duplicated == changeSerializer ? this : new LookupEntitySerializer<>(duplicated);
    }

    @Override
    public LookupEntity<E> createInstance() {
        return null;
    }

    @Override
    public LookupEntity<E> copy(LookupEntity<E> from) {
        return new LookupEntity<>(changeSerializer.copy(from.change()), from.relocation());
    }

    @Override
    public LookupEntity<E> copy(LookupEntity<E> from, LookupEntity<E> reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(LookupEntity<E> entity, DataOutputView target) throws IOException {
        target.writeBoolean(entity.relocation());
        changeSerializer.serialize(entity.change(), target);
    }

    @Override
    public LookupEntity<E> deserialize(DataInputView source) throws IOException {
        boolean relocation = source.readBoolean();
        return new LookupEntity<>(changeSerializer.deserialize(source), relocation);
    }

    @Override
    public LookupEntity<E> deserialize(LookupEntity<E> reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        target.writeBoolean(source.readBoolean());
        changeSerializer.copy(source, target);
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof LookupEntitySerializer<?> that && changeSerializer.equals(that.changeSerializer);
    }

    @Override
    public int hashCode() {
        return changeSerializer.hashCode();
    }

    @Override
    public TypeSerializerSnapshot<LookupEntity<E>> snapshotConfiguration() {
        return new Snapshot<>(this);
    }

    public static final class Snapshot<E> extends CompositeTypeSerializerSnapshot<LookupEntity<E>, LookupEntitySerializer<E>> {
        private static final int VERSION = 1;

        public Snapshot() {
        }

        Snapshot(LookupEntitySerializer<E> serializer) {
            super(serializer);
        }

        @Override
        protected int getCurrentOuterSnapshotVersion() {
            return VERSION;
        }

        @Override
        protected TypeSerializer<?>[] getNestedSerializers(LookupEntitySerializer<E> outerSerializer) {
            return new TypeSerializer<?>[] {outerSerializer.changeSerializer};
        }

        @Override
        @SuppressWarnings("unchecked")
        protected LookupEntitySerializer<E> createOuterSerializerWithNestedSerializers(TypeSerializer<?>[] nestedSerializers) {
            return new LookupEntitySerializer<>((TypeSerializer<Change<E>>) nestedSerializers[0]);
        }
    }
}
