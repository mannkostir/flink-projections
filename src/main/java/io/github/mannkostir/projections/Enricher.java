package io.github.mannkostir.projections;

import java.io.Serializable;

public interface Enricher<E, D, O> extends Serializable {
    O enrich(E entity, D dimension) throws Exception;
}
