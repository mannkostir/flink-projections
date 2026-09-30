package io.github.mannkostir.projections.elasticsearch;

import java.util.List;
import java.util.OptionalInt;

record RequestFailure(OptionalInt status, String reason) implements BulkOutcome {
    static RequestFailure withStatus(int status, String reason) {
        return new RequestFailure(OptionalInt.of(status), reason);
    }

    static RequestFailure unreachable(String reason) {
        return new RequestFailure(OptionalInt.empty(), reason);
    }

    boolean isTransient() {
        return status.isEmpty() || FailureClassifier.isTransient(status.getAsInt());
    }

    @Override
    public List<PendingOperation> retryable(List<PendingOperation> sent, String index) {
        if (!isTransient()) {
            throw ElasticsearchWriteException.permanentRequest(index, this);
        }
        return sent;
    }

    @Override
    public String describe() {
        return status.isPresent()
                ? "bulk request failed with status " + status.getAsInt() + ": " + reason
                : "bulk request could not reach Elasticsearch: " + reason;
    }
}
