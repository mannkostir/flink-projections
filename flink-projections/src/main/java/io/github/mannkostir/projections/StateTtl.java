package io.github.mannkostir.projections;

import java.time.Duration;
import java.util.Optional;

import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;

final class StateTtl {
    private StateTtl() {
    }

    static <D extends StateDescriptor<?, ?>> D applyTo(D descriptor, Optional<Duration> ttl) {
        ttl.map(StateTtl::config).ifPresent(descriptor::enableTimeToLive);
        return descriptor;
    }

    private static StateTtlConfig config(Duration ttl) {
        return StateTtlConfig.newBuilder(ttl)
                .updateTtlOnCreateAndWrite()
                .neverReturnExpired()
                .build();
    }
}
