package io.github.mannkostir.projections;

import java.io.IOException;
import java.util.Arrays;

import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

final class SlotChangeSerializer extends TypeSerializer<SlotChange> {
    private final TypeSerializer<Change<Object>>[] slotSerializers;

    SlotChangeSerializer(TypeSerializer<Change<Object>>[] slotSerializers) {
        this.slotSerializers = slotSerializers;
    }

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    @SuppressWarnings("unchecked")
    public TypeSerializer<SlotChange> duplicate() {
        TypeSerializer<Change<Object>>[] duplicated = new TypeSerializer[slotSerializers.length];
        boolean stateless = true;
        for (int slot = 0; slot < slotSerializers.length; slot++) {
            duplicated[slot] = slotSerializers[slot].duplicate();
            stateless &= duplicated[slot] == slotSerializers[slot];
        }
        return stateless ? this : new SlotChangeSerializer(duplicated);
    }

    @Override
    public SlotChange createInstance() {
        return null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public SlotChange copy(SlotChange from) {
        return new SlotChange(from.slot(), slotSerializers[from.slot()].copy((Change<Object>) from.change()));
    }

    @Override
    public SlotChange copy(SlotChange from, SlotChange reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void serialize(SlotChange record, DataOutputView target) throws IOException {
        target.writeInt(record.slot());
        slotSerializers[record.slot()].serialize((Change<Object>) record.change(), target);
    }

    @Override
    public SlotChange deserialize(DataInputView source) throws IOException {
        int slot = source.readInt();
        return new SlotChange(slot, slotSerializers[slot].deserialize(source));
    }

    @Override
    public SlotChange deserialize(SlotChange reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        int slot = source.readInt();
        target.writeInt(slot);
        slotSerializers[slot].copy(source, target);
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof SlotChangeSerializer that && Arrays.equals(slotSerializers, that.slotSerializers);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(slotSerializers);
    }

    @Override
    public TypeSerializerSnapshot<SlotChange> snapshotConfiguration() {
        return new Snapshot(this);
    }

    public static final class Snapshot extends CompositeTypeSerializerSnapshot<SlotChange, SlotChangeSerializer> {
        private static final int VERSION = 1;

        public Snapshot() {
        }

        Snapshot(SlotChangeSerializer serializer) {
            super(serializer);
        }

        @Override
        protected int getCurrentOuterSnapshotVersion() {
            return VERSION;
        }

        @Override
        protected TypeSerializer<?>[] getNestedSerializers(SlotChangeSerializer outerSerializer) {
            return outerSerializer.slotSerializers;
        }

        @Override
        @SuppressWarnings("unchecked")
        protected SlotChangeSerializer createOuterSerializerWithNestedSerializers(TypeSerializer<?>[] nestedSerializers) {
            TypeSerializer<Change<Object>>[] slots = new TypeSerializer[nestedSerializers.length];
            for (int slot = 0; slot < nestedSerializers.length; slot++) {
                slots[slot] = (TypeSerializer<Change<Object>>) nestedSerializers[slot];
            }
            return new SlotChangeSerializer(slots);
        }
    }
}
