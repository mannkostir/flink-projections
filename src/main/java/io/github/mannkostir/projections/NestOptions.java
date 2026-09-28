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
        private Duration orphanTimeout;

        private Builder() {
        }

        public Builder parentStateTtl(Duration ttl) {
            this.parentStateTtl = Durations.requirePositive(ttl, "parentStateTtl");
            return this;
        }

        public Builder orphanTimeout(Duration timeout) {
            this.orphanTimeout = Durations.requirePositive(timeout, "orphanTimeout");
            return this;
        }

        public NestOptions build() {
            return new NestOptions(parentStateTtl, orphanTimeout);
        }
    }
}
