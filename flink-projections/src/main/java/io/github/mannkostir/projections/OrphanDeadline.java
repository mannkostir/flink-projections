package io.github.mannkostir.projections;

import java.io.Serializable;
import java.time.Duration;
import java.util.List;

final class OrphanDeadline implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final long LONGEST_TIMEOUT_MILLIS = Long.MAX_VALUE / 4;

    private final long timeoutMillis;

    OrphanDeadline(Duration timeout) {
        this.timeoutMillis = boundedMillis(timeout);
    }

    long after(String key, long now) {
        long offset = Math.floorMod(key.hashCode(), timeoutMillis);
        return (Math.floorDiv(now - offset, timeoutMillis) + 2) * timeoutMillis + offset;
    }

    List<Long> superseded(long deadline) {
        return List.of(deadline - 2 * timeoutMillis, deadline - timeoutMillis);
    }

    private static long boundedMillis(Duration timeout) {
        if (timeout.compareTo(Duration.ofMillis(LONGEST_TIMEOUT_MILLIS)) > 0) {
            return LONGEST_TIMEOUT_MILLIS;
        }
        return Math.max(1L, timeout.toMillis());
    }
}
