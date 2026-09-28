package io.github.mannkostir.projections;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

import org.apache.flink.api.java.functions.KeySelector;

final class RoutingRules<T> implements Serializable {
    private final KeySelector<T, String> targetKey;

    RoutingRules(KeySelector<T, String> targetKey) {
        this.targetKey = targetKey;
    }

    List<Routed<T>> route(T lastValue, Change<T> incoming) throws Exception {
        if (incoming instanceof Upsert<T> && lastValue != null && movesTarget(lastValue, incoming.value())) {
            return List.of(new Routed<>(new Delete<>(incoming.id(), lastValue), true), new Routed<>(incoming, false));
        }
        return List.of(new Routed<>(incoming, false));
    }

    private boolean movesTarget(T lastValue, T newValue) throws Exception {
        return !Objects.equals(targetKey.getKey(lastValue), targetKey.getKey(newValue));
    }
}
