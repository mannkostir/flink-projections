package io.github.mannkostir.projections;

import java.io.Serializable;
import java.util.Map;
import java.util.function.Consumer;

final class LookupRules<E, D, O> implements Serializable {
    private final Enricher<E, D, O> enricher;
    private final boolean requireMatch;

    LookupRules(Enricher<E, D, O> enricher, boolean requireMatch) {
        this.enricher = enricher;
        this.requireMatch = requireMatch;
    }

    void onEntity(LookupEntity<E> entity, LookupState<E, D> state, Consumer<Change<O>> out) throws Exception {
        Change<E> change = entity.change();
        if (entity.relocation()) {
            state.removeEntity(change.id());
            return;
        }
        if (change instanceof Upsert<E> upsert) {
            state.putEntity(upsert.id(), upsert.value());
            emitIfMatchAllows(new Upsert<>(upsert.id(), enricher.enrich(upsert.value(), state.dimension())), state, out);
            return;
        }
        E stored = state.entity(change.id());
        if (stored != null) {
            state.removeEntity(change.id());
            emitIfMatchAllows(new Delete<>(change.id(), enricher.enrich(stored, state.dimension())), state, out);
        }
    }

    void onDimension(Change<D> change, LookupState<E, D> state, Consumer<Change<O>> out) throws Exception {
        if (change instanceof Upsert<D> upsert) {
            state.putDimension(upsert.value());
            reEmitAll(upsert.value(), state, out);
            return;
        }
        D previous = state.dimension();
        state.clearDimension();
        if (requireMatch) {
            deleteAllMatchedBy(previous, state, out);
        } else {
            reEmitAll(null, state, out);
        }
    }

    private void emitIfMatchAllows(Change<O> change, LookupState<E, D> state, Consumer<Change<O>> out) throws Exception {
        if (!requireMatch || state.dimension() != null) {
            out.accept(change);
        }
    }

    private void reEmitAll(D dimension, LookupState<E, D> state, Consumer<Change<O>> out) throws Exception {
        for (Map.Entry<String, E> entity : state.entities().entrySet()) {
            out.accept(new Upsert<>(entity.getKey(), enricher.enrich(entity.getValue(), dimension)));
        }
    }

    private void deleteAllMatchedBy(D previous, LookupState<E, D> state, Consumer<Change<O>> out) throws Exception {
        if (previous == null) {
            return;
        }
        for (Map.Entry<String, E> entity : state.entities().entrySet()) {
            out.accept(new Delete<>(entity.getKey(), enricher.enrich(entity.getValue(), previous)));
        }
    }
}
