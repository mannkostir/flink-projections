package io.github.mannkostir.projections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;

import org.junit.jupiter.api.Test;

class OptionsTest {
    @Test
    void defaultsHaveNoTtlNoOrphanTimeoutAndLeftJoin() {
        assertThat(NestOptions.defaults().parentStateTtl()).isEmpty();
        assertThat(NestOptions.defaults().orphanTimeout()).isEmpty();
        assertThat(ChildOptions.defaults().stateTtl()).isEmpty();
        assertThat(LookupOptions.defaults().stateTtl()).isEmpty();
        assertThat(LookupOptions.defaults().requireMatch()).isFalse();
    }

    @Test
    void rejectsZeroParentStateTtl() {
        assertThatThrownBy(() -> NestOptions.builder().parentStateTtl(Duration.ZERO).build())
                .isInstanceOf(ProjectionConfigurationException.class)
                .hasMessageContaining("NestOptions.parentStateTtl");
    }

    @Test
    void rejectsNegativeOrphanTimeout() {
        assertThatThrownBy(() -> NestOptions.builder().orphanTimeout(Duration.ofSeconds(-1)).build())
                .isInstanceOf(ProjectionConfigurationException.class)
                .hasMessageContaining("NestOptions.orphanTimeout");
    }

    @Test
    void rejectsNullChildStateTtl() {
        assertThatThrownBy(() -> ChildOptions.builder().stateTtl(null).build())
                .isInstanceOf(ProjectionConfigurationException.class)
                .hasMessageContaining("ChildOptions.stateTtl");
    }

    @Test
    void rejectsZeroLookupStateTtl() {
        assertThatThrownBy(() -> LookupOptions.builder().stateTtl(Duration.ZERO).build())
                .isInstanceOf(ProjectionConfigurationException.class)
                .hasMessageContaining("LookupOptions.stateTtl");
    }
}
