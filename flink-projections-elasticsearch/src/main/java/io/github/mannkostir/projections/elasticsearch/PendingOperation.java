package io.github.mannkostir.projections.elasticsearch;

sealed interface PendingOperation permits IndexOperation, DeleteOperation {
    long ACTION_OVERHEAD_BYTES = 64;

    String id();

    long sizeInBytes();

    boolean isSatisfiedDespite(ItemFailure failure);
}
