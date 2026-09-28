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

        private Builder() {
        }

        public Builder stateTtl(Duration ttl) {
            this.stateTtl = Durations.requirePositive(ttl, "stateTtl");
            return this;
        }

        public ChildOptions build() {
            return new ChildOptions(stateTtl);
        }
    }
}
