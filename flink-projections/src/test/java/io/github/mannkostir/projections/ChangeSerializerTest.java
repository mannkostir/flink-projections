package io.github.mannkostir.projections;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;

import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.types.StringValue;
import org.junit.jupiter.api.Test;

class ChangeSerializerTest extends SerializerTestBase<Change<String>> {
    @Test
    void unknownKindFails() throws Exception {
        DataOutputSerializer output = new DataOutputSerializer(64);
        output.writeByte(2);
        StringValue.writeString("c1", output);
        StringSerializer.INSTANCE.serialize("alice", output);

        DataInputDeserializer input = new DataInputDeserializer(output.getCopyOfBuffer());

        assertThatThrownBy(() -> new ChangeSerializer<>(StringSerializer.INSTANCE).deserialize(input))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Unknown Change kind 2");
    }

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
