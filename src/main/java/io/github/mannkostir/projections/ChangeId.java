package io.github.mannkostir.projections;

import org.apache.flink.api.java.functions.KeySelector;

final class ChangeId<T> implements KeySelector<Change<T>, String> {
    @Override
    public String getKey(Change<T> change) {
        return change.id();
    }
}
