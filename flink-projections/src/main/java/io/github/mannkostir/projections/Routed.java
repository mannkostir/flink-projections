package io.github.mannkostir.projections;

record Routed<T>(Change<T> change, boolean relocation) {
}
