package io.github.mannkostir.projections;

import java.util.Objects;

import org.apache.flink.api.common.serialization.SerializerConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

final class ChangeTypeInfo<T> extends TypeInformation<Change<T>> {
    private final TypeInformation<T> valueType;

    ChangeTypeInfo(TypeInformation<T> valueType) {
        this.valueType = Objects.requireNonNull(valueType, "valueType");
    }

    TypeInformation<T> valueType() {
        return valueType;
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
    @SuppressWarnings({"unchecked", "rawtypes"})
    public Class<Change<T>> getTypeClass() {
        return (Class) Change.class;
    }

    @Override
    public boolean isKeyType() {
        return false;
    }

    @Override
    public TypeSerializer<Change<T>> createSerializer(SerializerConfig config) {
        return new ChangeSerializer<>(valueType.createSerializer(config));
    }

    @Override
    public String toString() {
        return "Change<" + valueType + ">";
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof ChangeTypeInfo<?> that && that.canEqual(this) && valueType.equals(that.valueType);
    }

    @Override
    public int hashCode() {
        return valueType.hashCode();
    }

    @Override
    public boolean canEqual(Object other) {
        return other instanceof ChangeTypeInfo;
    }
}
