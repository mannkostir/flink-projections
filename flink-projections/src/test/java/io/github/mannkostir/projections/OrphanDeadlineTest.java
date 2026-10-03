package io.github.mannkostir.projections;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;

import org.junit.jupiter.api.Test;

class OrphanDeadlineTest {
    private static final OrphanDeadline EVERY_HUNDRED_MILLIS = new OrphanDeadline(Duration.ofMillis(100));
    private static final String KEY_WITH_OFFSET_18 = "c1";
    private static final String KEY_WITH_OFFSET_19 = "c2";

    @Test
    void deadlineIsEndOfFollowingBucketOnTheKeysGrid() {
        assertThat(EVERY_HUNDRED_MILLIS.after(KEY_WITH_OFFSET_18, 140L)).isEqualTo(318L);
    }

    @Test
    void deadlineAtBucketBoundaryIsEndOfFollowingBucket() {
        assertThat(EVERY_HUNDRED_MILLIS.after(KEY_WITH_OFFSET_19, 119L)).isEqualTo(319L);
    }

    @Test
    void keysWithDifferentOffsetsGetDifferentDeadlines() {
        assertThat(EVERY_HUNDRED_MILLIS.after(KEY_WITH_OFFSET_18, 140L))
                .isNotEqualTo(EVERY_HUNDRED_MILLIS.after(KEY_WITH_OFFSET_19, 140L));
    }

    @Test
    void supersededDeadlinesIncludeOneStillDueButNotYetFired() {
        assertThat(EVERY_HUNDRED_MILLIS.superseded(318L)).containsExactly(118L, 218L);
    }

    @Test
    void subMillisecondTimeoutUsesOneMillisecondBuckets() {
        assertThat(new OrphanDeadline(Duration.ofNanos(1)).after(KEY_WITH_OFFSET_19, 5L)).isEqualTo(7L);
    }

    @Test
    void hugeTimeoutDoesNotOverflowIntoThePast() {
        assertThat(new OrphanDeadline(Duration.ofSeconds(Long.MAX_VALUE)).after(KEY_WITH_OFFSET_19, 5L))
                .isGreaterThan(5L);
    }
}
