package io.github.mannkostir.projections;

import java.util.Objects;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;

public final class Changes {
    private Changes() {
    }

    public static <T> TypeInformation<Change<T>> typeInfo(TypeInformation<T> type) {
        return new ChangeTypeInfo<>(type);
    }

    public static <T> DataStream<Change<T>> from(
            String name,
            DataStream<T> stream,
            KeySelector<T, String> id,
            FilterFunction<T> isDeleted,
            TypeInformation<T> type) {
        Objects.requireNonNull(stream, "stream");
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(isDeleted, "isDeleted");
        Objects.requireNonNull(type, "type");
        String uid = ContractNames.changesUid(Names.requireValid(name, "Changes name"));
        return stream.map(new ToChange<>(id, isDeleted), typeInfo(type)).uid(uid).name(uid);
    }
}
