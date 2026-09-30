package io.github.mannkostir.projections.elasticsearch;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;

import org.junit.jupiter.api.Test;

class BackoffTest {
    private final Backoff backoff = new Backoff(Duration.ofMillis(100), Duration.ofMillis(500));

    @Test
    void firstRetryWaitsTheInitialDelay() {
        assertThat(backoff.delayFor(1)).isEqualTo(Duration.ofMillis(100));
    }

    @Test
    void delayDoublesPerRetry() {
        assertThat(backoff.delayFor(3)).isEqualTo(Duration.ofMillis(400));
    }

    @Test
    void delayIsCappedAtMax() {
        assertThat(backoff.delayFor(4)).isEqualTo(Duration.ofMillis(500));
    }

    @Test
    void largeAttemptsStayAtMax() {
        assertThat(backoff.delayFor(10_000)).isEqualTo(Duration.ofMillis(500));
    }
}
