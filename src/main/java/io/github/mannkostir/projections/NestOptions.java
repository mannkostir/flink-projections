package io.github.mannkostir.projections;

import java.io.Serializable;
import java.time.Duration;
import java.util.Optional;

public final class NestOptions implements Serializable {
    private final Duration parentStateTtl;
    private final Duration orphanTimeout;

    private NestOptions(Duration parentStateTtl, Duration orphanTimeout) {
        this.parentStateTtl = parentStateTtl;
        this.orphanTimeout = orphanTimeout;
    }

    public static NestOptions defaults() {
        return builder().build();
    }

    public static Builder builder() {
        return new Builder();
    }

    Optional<Duration> parentStateTtl() {
        return Optional.ofNullable(parentStateTtl);
    }

    Optional<Duration> orphanTimeout() {
        return Optional.ofNullable(orphanTimeout);
    }

    public static final class Builder {
        private Duration parentStateTtl;
        private boolean parentStateTtlSet;
        private Duration orphanTimeout;
        private boolean orphanTimeoutSet;

        private Builder() {
        }

        public Builder parentStateTtl(Duration ttl) {
            this.parentStateTtl = ttl;
            this.parentStateTtlSet = true;
            return this;
        }

        public Builder orphanTimeout(Duration timeout) {
            this.orphanTimeout = timeout;
            this.orphanTimeoutSet = true;
            return this;
        }

        public NestOptions build() {
            return new NestOptions(
                    validated(parentStateTtlSet, parentStateTtl, "parentStateTtl"),
                    validated(orphanTimeoutSet, orphanTimeout, "orphanTimeout"));
        }

        private static Duration validated(boolean set, Duration duration, String option) {
            return set ? Durations.requirePositive(duration, option) : null;
        }
    }
}
