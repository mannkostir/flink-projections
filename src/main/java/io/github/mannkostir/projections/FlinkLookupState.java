package io.github.mannkostir.projections;

import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.ValueState;

final class FlinkLookupState<E, D> implements LookupState<E, D> {
    private final MapState<String, E> entities;
    private final ValueState<D> dimension;

    FlinkLookupState(MapState<String, E> entities, ValueState<D> dimension) {
        this.entities = entities;
        this.dimension = dimension;
    }

    @Override
    public D dimension() throws Exception {
        return dimension.value();
    }

    @Override
    public void putDimension(D value) throws Exception {
        dimension.update(value);
    }

    @Override
    public void clearDimension() {
        dimension.clear();
    }

    @Override
    public E entity(String entityId) throws Exception {
        return entities.get(entityId);
    }

    @Override
    public void putEntity(String entityId, E entity) throws Exception {
        entities.put(entityId, entity);
    }

    @Override
    public void removeEntity(String entityId) throws Exception {
        entities.remove(entityId);
    }

    @Override
    public SortedMap<String, E> entities() throws Exception {
        SortedMap<String, E> sorted = new TreeMap<>();
        for (Map.Entry<String, E> entry : entities.entries()) {
            sorted.put(entry.getKey(), entry.getValue());
        }
        return sorted;
    }
}
