package io.github.mannkostir.projections;

import java.io.Serializable;
import java.time.Duration;
import java.util.Optional;

public final class LookupOptions implements Serializable {
    private final Duration stateTtl;
    private final boolean requireMatch;

    private LookupOptions(Duration stateTtl, boolean requireMatch) {
        this.stateTtl = stateTtl;
        this.requireMatch = requireMatch;
    }

    public static LookupOptions defaults() {
        return builder().build();
    }

    public static Builder builder() {
        return new Builder();
    }

    Optional<Duration> stateTtl() {
        return Optional.ofNullable(stateTtl);
    }

    boolean requireMatch() {
        return requireMatch;
    }

    public static final class Builder {
        private Duration stateTtl;
        private boolean stateTtlSet;
        private boolean requireMatch;

        private Builder() {
        }

        public Builder stateTtl(Duration ttl) {
            this.stateTtl = ttl;
            this.stateTtlSet = true;
            return this;
        }

        public Builder requireMatch(boolean requireMatch) {
            this.requireMatch = requireMatch;
            return this;
        }

        public LookupOptions build() {
            return new LookupOptions(
                    Durations.requirePositiveIfSet(stateTtlSet, stateTtl, "LookupOptions.stateTtl"), requireMatch);
        }
    }
}
