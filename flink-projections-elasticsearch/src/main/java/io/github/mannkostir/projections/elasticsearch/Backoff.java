package io.github.mannkostir.projections.elasticsearch;

import java.io.Serializable;
import java.time.Duration;

record Backoff(Duration initial, Duration max) implements Serializable {
    Duration delayFor(int attempt) {
        Duration delay = initial;
        for (int step = 1; step < attempt && delay.compareTo(max) < 0; step++) {
            delay = delay.multipliedBy(2);
        }
        return delay.compareTo(max) < 0 ? delay : max;
    }
}
