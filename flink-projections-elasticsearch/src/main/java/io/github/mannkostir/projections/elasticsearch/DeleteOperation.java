package io.github.mannkostir.projections.elasticsearch;

import java.util.Objects;

record DeleteOperation(String id) implements PendingOperation {
    DeleteOperation {
        Objects.requireNonNull(id, "id");
    }

    @Override
    public long sizeInBytes() {
        return ACTION_OVERHEAD_BYTES;
    }

    @Override
    public boolean isSatisfiedDespite(ItemFailure failure) {
        return failure.isMissingIndex();
    }
}
