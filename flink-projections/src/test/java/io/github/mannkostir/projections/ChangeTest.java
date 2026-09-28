package io.github.mannkostir.projections;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class ChangeTest {
    @Test
    void upsertRejectsBlankId() {
        assertThatThrownBy(() -> new Upsert<>(" ", "value"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("id");
    }

    @Test
    void upsertRejectsNullValue() {
        assertThatThrownBy(() -> new Upsert<>("c1", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("value");
    }

    @Test
    void deleteRejectsNullId() {
        assertThatThrownBy(() -> new Delete<>(null, "value"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("id");
    }

    @Test
    void deleteRejectsNullValue() {
        assertThatThrownBy(() -> new Delete<>("c1", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("value");
    }
}
