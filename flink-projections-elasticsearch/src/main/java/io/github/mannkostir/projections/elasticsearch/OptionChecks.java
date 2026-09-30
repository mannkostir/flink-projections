package io.github.mannkostir.projections.elasticsearch;

import java.time.Duration;

import io.github.mannkostir.projections.ProjectionConfigurationException;

final class OptionChecks {
    private OptionChecks() {
    }

    static String requireText(String value, String option) {
        if (value == null || value.isBlank()) {
            throw new ProjectionConfigurationException(option + " is required: set it to a non-blank value");
        }
        return value;
    }

    static <T> T requirePresent(T value, String option) {
        if (value == null) {
            throw new ProjectionConfigurationException(option + " must not be null: omit the call to keep the default");
        }
        return value;
    }

    static long requirePositive(long value, String option) {
        if (value <= 0) {
            throw new ProjectionConfigurationException(option + " must be greater than 0, but was " + value);
        }
        return value;
    }

    static int requireNotNegative(int value, String option) {
        if (value < 0) {
            throw new ProjectionConfigurationException(option + " must be 0 or greater, but was " + value);
        }
        return value;
    }

    static Duration requirePositive(Duration value, String option) {
        requirePresent(value, option);
        if (value.isZero() || value.isNegative()) {
            throw new ProjectionConfigurationException(option + " must be a positive duration, but was " + value);
        }
        return value;
    }
}
