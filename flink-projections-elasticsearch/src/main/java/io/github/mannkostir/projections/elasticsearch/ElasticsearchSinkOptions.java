package io.github.mannkostir.projections.elasticsearch;

import java.io.Serializable;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;

import io.github.mannkostir.projections.ProjectionConfigurationException;

public final class ElasticsearchSinkOptions implements Serializable {
    private static final String OPTIONS = "ElasticsearchSinkOptions";

    private final List<String> hosts;
    private final String index;
    private final ElasticsearchAuth auth;
    private final int maxBatchActions;
    private final long maxBatchBytes;
    private final Duration flushInterval;
    private final int maxRetries;
    private final Backoff retryBackoff;

    private ElasticsearchSinkOptions(Builder builder) {
        this.hosts = Hosts.requireValid(builder.hosts);
        this.index = OptionChecks.requireText(builder.index, OPTIONS + ".index");
        this.auth = OptionChecks.requirePresent(builder.auth, OPTIONS + ".auth");
        this.maxBatchActions = (int) OptionChecks.requirePositive(builder.maxBatchActions, OPTIONS + ".maxBatchActions");
        this.maxBatchBytes = OptionChecks.requirePositive(builder.maxBatchBytes, OPTIONS + ".maxBatchBytes");
        this.flushInterval = OptionChecks.requirePositive(builder.flushInterval, OPTIONS + ".flushInterval");
        this.maxRetries = OptionChecks.requireNotNegative(builder.maxRetries, OPTIONS + ".maxRetries");
        this.retryBackoff = backoff(builder.initialBackoff, builder.maxBackoff);
    }

    private static Backoff backoff(Duration initial, Duration max) {
        OptionChecks.requirePositive(initial, OPTIONS + ".retryBackoff initial");
        OptionChecks.requirePositive(max, OPTIONS + ".retryBackoff max");
        if (initial.compareTo(max) > 0) {
            throw new ProjectionConfigurationException(
                    OPTIONS + ".retryBackoff initial " + initial + " is greater than max " + max + ": pass initial <= max");
        }
        return new Backoff(initial, max);
    }

    public static Builder builder() {
        return new Builder();
    }

    List<String> hosts() {
        return hosts;
    }

    String index() {
        return index;
    }

    ElasticsearchAuth auth() {
        return auth;
    }

    int maxBatchActions() {
        return maxBatchActions;
    }

    long maxBatchBytes() {
        return maxBatchBytes;
    }

    Duration flushInterval() {
        return flushInterval;
    }

    int maxRetries() {
        return maxRetries;
    }

    Backoff retryBackoff() {
        return retryBackoff;
    }

    public static final class Builder {
        private List<String> hosts = List.of();
        private String index;
        private ElasticsearchAuth auth = ElasticsearchAuth.none();
        private int maxBatchActions = 1000;
        private long maxBatchBytes = 5L * 1024 * 1024;
        private Duration flushInterval = Duration.ofSeconds(1);
        private int maxRetries = 8;
        private Duration initialBackoff = Duration.ofMillis(100);
        private Duration maxBackoff = Duration.ofSeconds(10);

        private Builder() {
        }

        public Builder hosts(String... hosts) {
            this.hosts = hosts == null ? List.of() : Arrays.asList(hosts.clone());
            return this;
        }

        public Builder index(String index) {
            this.index = index;
            return this;
        }

        public Builder auth(ElasticsearchAuth auth) {
            this.auth = auth;
            return this;
        }

        public Builder maxBatchActions(int maxBatchActions) {
            this.maxBatchActions = maxBatchActions;
            return this;
        }

        public Builder maxBatchBytes(long maxBatchBytes) {
            this.maxBatchBytes = maxBatchBytes;
            return this;
        }

        public Builder flushInterval(Duration flushInterval) {
            this.flushInterval = flushInterval;
            return this;
        }

        public Builder maxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }

        public Builder retryBackoff(Duration initial, Duration max) {
            this.initialBackoff = initial;
            this.maxBackoff = max;
            return this;
        }

        public ElasticsearchSinkOptions build() {
            return new ElasticsearchSinkOptions(this);
        }
    }
}
