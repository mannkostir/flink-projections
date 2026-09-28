package io.github.mannkostir.projections;

import java.util.List;

import org.apache.flink.api.common.serialization.SerializerConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

final class SlotChangeTypeInfo extends TypeInformation<SlotChange> {
    private final List<TypeInformation<?>> slotValueTypes;

    SlotChangeTypeInfo(List<TypeInformation<?>> slotValueTypes) {
        this.slotValueTypes = List.copyOf(slotValueTypes);
    }

    @Override
    public boolean isBasicType() {
        return false;
    }

    @Override
    public boolean isTupleType() {
        return false;
    }

    @Override
    public int getArity() {
        return 1;
    }

    @Override
    public int getTotalFields() {
        return 1;
    }

    @Override
    public Class<SlotChange> getTypeClass() {
        return SlotChange.class;
    }

    @Override
    public boolean isKeyType() {
        return false;
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public TypeSerializer<SlotChange> createSerializer(SerializerConfig config) {
        TypeSerializer<Change<Object>>[] slots = new TypeSerializer[slotValueTypes.size()];
        for (int slot = 0; slot < slots.length; slot++) {
            slots[slot] = new ChangeSerializer<>((TypeSerializer<Object>) slotValueTypes.get(slot).createSerializer(config));
        }
        return new SlotChangeSerializer(slots);
    }

    @Override
    public String toString() {
        return "SlotChange" + slotValueTypes;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof SlotChangeTypeInfo that && that.canEqual(this) && slotValueTypes.equals(that.slotValueTypes);
    }

    @Override
    public int hashCode() {
        return slotValueTypes.hashCode();
    }

    @Override
    public boolean canEqual(Object other) {
        return other instanceof SlotChangeTypeInfo;
    }
}
