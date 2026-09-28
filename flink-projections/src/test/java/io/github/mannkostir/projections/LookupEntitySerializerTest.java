package io.github.mannkostir.projections;

import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;

class LookupEntitySerializerTest extends SerializerTestBase<LookupEntity<String>> {
    @Override
    protected TypeSerializer<LookupEntity<String>> createSerializer() {
        return new LookupEntitySerializer<>(new ChangeSerializer<>(StringSerializer.INSTANCE));
    }

    @Override
    protected int getLength() {
        return -1;
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    protected Class<LookupEntity<String>> getTypeClass() {
        return (Class) LookupEntity.class;
    }

    @Override
    protected boolean allowNullInstances(TypeSerializer<LookupEntity<String>> serializer) {
        return true;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected LookupEntity<String>[] getTestData() {
        return new LookupEntity[] {
                new LookupEntity<>(new Upsert<>("e1", "dev"), false),
                new LookupEntity<>(new Delete<>("e1", "dev"), true),
                new LookupEntity<>(new Delete<>("e2", "qa"), false)
        };
    }
}
