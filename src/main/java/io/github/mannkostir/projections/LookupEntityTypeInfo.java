package io.github.mannkostir.projections;

import org.apache.flink.api.common.serialization.SerializerConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

final class LookupEntityTypeInfo<E> extends TypeInformation<LookupEntity<E>> {
    private final TypeInformation<E> entityType;

    LookupEntityTypeInfo(TypeInformation<E> entityType) {
        this.entityType = entityType;
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
    public Class<LookupEntity<E>> getTypeClass() {
        return (Class) LookupEntity.class;
    }

    @Override
    public boolean isKeyType() {
        return false;
    }

    @Override
    public TypeSerializer<LookupEntity<E>> createSerializer(SerializerConfig config) {
        return new LookupEntitySerializer<>(new ChangeSerializer<>(entityType.createSerializer(config)));
    }

    @Override
    public String toString() {
        return "LookupEntity<" + entityType + ">";
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof LookupEntityTypeInfo<?> that && that.canEqual(this) && entityType.equals(that.entityType);
    }

    @Override
    public int hashCode() {
        return entityType.hashCode();
    }

    @Override
    public boolean canEqual(Object other) {
        return other instanceof LookupEntityTypeInfo;
    }
}
