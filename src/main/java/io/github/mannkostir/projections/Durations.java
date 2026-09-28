package io.github.mannkostir.projections;

import java.time.Duration;

final class Durations {
    private Durations() {
    }

    static Duration requirePositive(Duration duration, String option) {
        if (duration == null || duration.isZero() || duration.isNegative()) {
            throw new ProjectionConfigurationException(option + " must be a positive duration, got: " + duration);
        }
        return duration;
    }

    static Duration requirePositiveIfSet(boolean set, Duration duration, String option) {
        return set ? requirePositive(duration, option) : null;
    }
}
