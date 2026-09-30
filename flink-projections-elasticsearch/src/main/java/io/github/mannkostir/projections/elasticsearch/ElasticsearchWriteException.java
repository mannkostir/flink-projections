package io.github.mannkostir.projections.elasticsearch;

public final class ElasticsearchWriteException extends RuntimeException {
    private ElasticsearchWriteException(String message) {
        super(message);
    }

    static ElasticsearchWriteException permanentItem(String index, ItemFailure failure) {
        return new ElasticsearchWriteException(
                "Writing to index '" + index + "' failed: " + failure.describe() + ". To fix: " + FailureHints.forItem(failure));
    }

    static ElasticsearchWriteException permanentRequest(String index, RequestFailure failure) {
        return new ElasticsearchWriteException(
                "Writing to index '" + index + "' failed: " + failure.describe() + ". To fix: " + FailureHints.forRequest(failure));
    }

    static ElasticsearchWriteException retriesExhausted(String index, int maxRetries, BulkOutcome last) {
        return new ElasticsearchWriteException(
                "Writing to index '" + index + "' still failed after " + maxRetries + " retries: " + last.describe()
                        + ". To fix: check cluster health, or raise ElasticsearchSinkOptions.maxRetries(...) or retryBackoff(...)");
    }
}
