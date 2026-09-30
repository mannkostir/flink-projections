package io.github.mannkostir.projections.elasticsearch;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class OperationBuffer {
    private final Map<String, PendingOperation> operations = new LinkedHashMap<>();
    private long bytes;

    void add(PendingOperation operation) {
        PendingOperation replaced = operations.put(operation.id(), operation);
        if (replaced != null) {
            bytes -= replaced.sizeInBytes();
        }
        bytes += operation.sizeInBytes();
    }

    boolean isFull(int maxActions, long maxBytes) {
        return operations.size() >= maxActions || bytes >= maxBytes;
    }

    boolean isEmpty() {
        return operations.isEmpty();
    }

    int actions() {
        return operations.size();
    }

    long bytes() {
        return bytes;
    }

    List<PendingOperation> drain() {
        List<PendingOperation> drained = List.copyOf(operations.values());
        operations.clear();
        bytes = 0;
        return drained;
    }
}
