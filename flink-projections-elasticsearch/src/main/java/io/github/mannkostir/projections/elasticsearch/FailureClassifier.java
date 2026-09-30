package io.github.mannkostir.projections.elasticsearch;

import java.util.Set;

final class FailureClassifier {
    private static final Set<Integer> TRANSIENT_STATUSES = Set.of(429, 502, 503, 504);

    private FailureClassifier() {
    }

    static boolean isTransient(int status) {
        return TRANSIENT_STATUSES.contains(status);
    }
}
