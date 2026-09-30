package io.github.mannkostir.projections.elasticsearch;

import java.util.List;

sealed interface BulkOutcome permits ItemResults, RequestFailure {
    List<PendingOperation> retryable(List<PendingOperation> sent, String index);

    String describe();
}
