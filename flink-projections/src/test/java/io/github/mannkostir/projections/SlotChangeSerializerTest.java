package io.github.mannkostir.projections;

import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;

class SlotChangeSerializerTest extends SerializerTestBase<SlotChange> {
    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    protected TypeSerializer<SlotChange> createSerializer() {
        return new SlotChangeSerializer(new TypeSerializer[] {
                new ChangeSerializer<>(StringSerializer.INSTANCE),
                new ChangeSerializer<>(LongSerializer.INSTANCE)
        });
    }

    @Override
    protected int getLength() {
        return -1;
    }

    @Override
    protected Class<SlotChange> getTypeClass() {
        return SlotChange.class;
    }

    @Override
    protected boolean allowNullInstances(TypeSerializer<SlotChange> serializer) {
        return true;
    }

    @Override
    protected SlotChange[] getTestData() {
        return new SlotChange[] {
                new SlotChange(0, new Upsert<>("s1", "java")),
                new SlotChange(0, new Delete<>("s1", "java")),
                new SlotChange(1, new Upsert<>("j1", 42L)),
                new SlotChange(1, new Delete<>("j2", -1L))
        };
    }
}
