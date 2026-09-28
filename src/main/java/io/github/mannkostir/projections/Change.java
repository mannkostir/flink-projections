package io.github.mannkostir.projections;

public sealed interface Change<T> permits Upsert, Delete {
    String id();

    T value();
}
