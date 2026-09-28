package io.github.mannkostir.projections;

import java.util.SortedMap;
import java.util.TreeMap;

final class InMemoryLookupState<E, D> implements LookupState<E, D> {
    private final TreeMap<String, E> entities = new TreeMap<>();
    private D dimension;

    @Override
    public D dimension() {
        return dimension;
    }

    @Override
    public void putDimension(D value) {
        dimension = value;
    }

    @Override
    public void clearDimension() {
        dimension = null;
    }

    @Override
    public E entity(String entityId) {
        return entities.get(entityId);
    }

    @Override
    public void putEntity(String entityId, E entity) {
        entities.put(entityId, entity);
    }

    @Override
    public void removeEntity(String entityId) {
        entities.remove(entityId);
    }

    @Override
    public SortedMap<String, E> entities() {
        return new TreeMap<>(entities);
    }
}
