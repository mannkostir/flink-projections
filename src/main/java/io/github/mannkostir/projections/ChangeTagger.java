package io.github.mannkostir.projections;

import java.io.Serializable;

interface ChangeTagger<T, R> extends Serializable {
    R tag(Routed<T> routed);
}
