package io.github.mannkostir.projections.elasticsearch;

import java.util.Arrays;
import java.util.Objects;

record IndexOperation(String id, byte[] document) implements PendingOperation {
    IndexOperation {
        Objects.requireNonNull(id, "id");
        document = Objects.requireNonNull(document, "document").clone();
    }

    @Override
    public byte[] document() {
        return document.clone();
    }

    @Override
    public long sizeInBytes() {
        return document.length + ACTION_OVERHEAD_BYTES;
    }

    @Override
    public boolean isSatisfiedDespite(ItemFailure failure) {
        return false;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof IndexOperation that && id.equals(that.id) && Arrays.equals(document, that.document);
    }

    @Override
    public int hashCode() {
        return 31 * id.hashCode() + Arrays.hashCode(document);
    }

    @Override
    public String toString() {
        return "IndexOperation[id=" + id + ", bytes=" + document.length + "]";
    }
}
