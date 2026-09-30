package io.github.mannkostir.projections.elasticsearch;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

record ItemResults(List<ItemFailure> failures) implements BulkOutcome {
    ItemResults {
        failures = List.copyOf(failures);
    }

    static ItemResults allSucceeded() {
        return new ItemResults(List.of());
    }

    @Override
    public List<PendingOperation> retryable(List<PendingOperation> sent, String index) {
        Map<String, PendingOperation> sentById = sent.stream().collect(Collectors.toMap(PendingOperation::id, Function.identity()));
        List<ItemFailure> unsatisfied = failures.stream()
                .filter(failure -> !sentById.get(failure.id()).isSatisfiedDespite(failure))
                .toList();
        unsatisfied.stream()
                .filter(failure -> !failure.isTransient())
                .findFirst()
                .ifPresent(failure -> {
                    throw ElasticsearchWriteException.permanentItem(index, failure);
                });
        Set<String> transientIds = unsatisfied.stream().map(ItemFailure::id).collect(Collectors.toSet());
        return sent.stream().filter(operation -> transientIds.contains(operation.id())).toList();
    }

    @Override
    public String describe() {
        return failures.size() + " item(s) failed, first: " + failures.get(0).describe();
    }
}
