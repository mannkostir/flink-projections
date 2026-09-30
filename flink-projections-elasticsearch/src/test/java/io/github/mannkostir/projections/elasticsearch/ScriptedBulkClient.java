package io.github.mannkostir.projections.elasticsearch;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

final class ScriptedBulkClient implements BulkClient {
    private final Deque<BulkOutcome> outcomes = new ArrayDeque<>();
    private final List<List<PendingOperation>> sent = new ArrayList<>();
    private boolean closed;

    ScriptedBulkClient thenRespond(BulkOutcome outcome) {
        outcomes.add(outcome);
        return this;
    }

    @Override
    public BulkOutcome send(List<PendingOperation> operations) {
        sent.add(List.copyOf(operations));
        return outcomes.isEmpty() ? ItemResults.allSucceeded() : outcomes.poll();
    }

    @Override
    public void close() {
        closed = true;
    }

    List<List<PendingOperation>> sent() {
        return List.copyOf(sent);
    }

    boolean closed() {
        return closed;
    }
}
