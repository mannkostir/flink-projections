package io.github.mannkostir.projections.elasticsearch;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class BulkDispatcher {
    private static final Logger LOG = LoggerFactory.getLogger(BulkDispatcher.class);

    private final BulkClient client;
    private final String index;
    private final int maxRetries;
    private final Backoff backoff;
    private final Sleeper sleeper;

    BulkDispatcher(BulkClient client, String index, int maxRetries, Backoff backoff, Sleeper sleeper) {
        this.client = client;
        this.index = index;
        this.maxRetries = maxRetries;
        this.backoff = backoff;
        this.sleeper = sleeper;
    }

    void dispatch(List<PendingOperation> operations) throws InterruptedException {
        List<PendingOperation> pending = operations;
        int retries = 0;
        while (!pending.isEmpty()) {
            BulkOutcome outcome = client.send(pending);
            List<PendingOperation> retryable = outcome.retryable(pending, index);
            if (retryable.isEmpty()) {
                return;
            }
            if (retries == maxRetries) {
                throw ElasticsearchWriteException.retriesExhausted(index, maxRetries, outcome);
            }
            retries++;
            LOG.warn("Retrying {} operation(s) on index '{}' (retry {} of {}): {}", retryable.size(), index, retries, maxRetries, outcome.describe());
            sleeper.sleep(backoff.delayFor(retries));
            pending = retryable;
        }
    }
}
