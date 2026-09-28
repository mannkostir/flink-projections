package io.github.mannkostir.projections;

import java.io.Serializable;
import java.time.Duration;
import java.util.Optional;

public final class ChildOptions implements Serializable {
    private final Duration stateTtl;

    private ChildOptions(Duration stateTtl) {
        this.stateTtl = stateTtl;
    }

    public static ChildOptions defaults() {
        return builder().build();
    }

    public static Builder builder() {
        return new Builder();
    }

    Optional<Duration> stateTtl() {
        return Optional.ofNullable(stateTtl);
    }

    public static final class Builder {
        private Duration stateTtl;
        private boolean stateTtlSet;

        private Builder() {
        }

        public Builder stateTtl(Duration ttl) {
            this.stateTtl = ttl;
            this.stateTtlSet = true;
            return this;
        }

        public ChildOptions build() {
            return new ChildOptions(validated(stateTtlSet, stateTtl, "stateTtl"));
        }

        private static Duration validated(boolean set, Duration duration, String option) {
            return set ? Durations.requirePositive(duration, option) : null;
        }
    }
}
