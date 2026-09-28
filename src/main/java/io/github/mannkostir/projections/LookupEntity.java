package io.github.mannkostir.projections;

record LookupEntity<E>(Change<E> change, boolean relocation) {
}
