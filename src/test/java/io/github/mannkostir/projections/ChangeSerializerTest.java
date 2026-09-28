package io.github.mannkostir.projections;

import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;

class ChangeSerializerTest extends SerializerTestBase<Change<String>> {
    @Override
    protected TypeSerializer<Change<String>> createSerializer() {
        return new ChangeSerializer<>(StringSerializer.INSTANCE);
    }

    @Override
    protected int getLength() {
        return -1;
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    protected Class<Change<String>> getTypeClass() {
        return (Class) Change.class;
    }

    @Override
    protected boolean allowNullInstances(TypeSerializer<Change<String>> serializer) {
        return true;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Change<String>[] getTestData() {
        return new Change[] {
                new Upsert<>("c1", "alice"),
                new Delete<>("c1", "alice"),
                new Upsert<>("c-2", ""),
                new Delete<>("c3", "日本")
        };
    }
}
