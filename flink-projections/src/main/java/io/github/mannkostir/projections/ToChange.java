package io.github.mannkostir.projections;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;

final class ToChange<T> implements MapFunction<T, Change<T>> {
    private final KeySelector<T, String> id;
    private final FilterFunction<T> isDeleted;

    ToChange(KeySelector<T, String> id, FilterFunction<T> isDeleted) {
        this.id = id;
        this.isDeleted = isDeleted;
    }

    @Override
    public Change<T> map(T value) throws Exception {
        String changeId = id.getKey(value);
        return isDeleted.filter(value) ? new Delete<>(changeId, value) : new Upsert<>(changeId, value);
    }
}
