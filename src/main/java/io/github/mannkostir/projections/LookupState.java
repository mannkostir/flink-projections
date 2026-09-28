package io.github.mannkostir.projections;

import java.util.SortedMap;

interface LookupState<E, D> {
    D dimension() throws Exception;

    void putDimension(D dimension) throws Exception;

    void clearDimension() throws Exception;

    E entity(String entityId) throws Exception;

    void putEntity(String entityId, E entity) throws Exception;

    void removeEntity(String entityId) throws Exception;

    SortedMap<String, E> entities() throws Exception;
}
