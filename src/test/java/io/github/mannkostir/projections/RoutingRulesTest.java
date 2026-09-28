package io.github.mannkostir.projections;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class RoutingRulesTest {
    private final RoutingRules<String> rules = new RoutingRules<>(value -> value.split("@")[1]);

    @Test
    void firstUpsertIsForwarded() throws Exception {
        assertThat(rules.route(null, new Upsert<>("e1", "e1@a")))
                .containsExactly(new Routed<>(new Upsert<>("e1", "e1@a"), false));
    }

    @Test
    void upsertToSameTargetIsForwarded() throws Exception {
        assertThat(rules.route("e1@a", new Upsert<>("e1", "e1-renamed@a")))
                .containsExactly(new Routed<>(new Upsert<>("e1", "e1-renamed@a"), false));
    }

    @Test
    void upsertToNewTargetRelocatesFromOldTargetFirst() throws Exception {
        assertThat(rules.route("e1@a", new Upsert<>("e1", "e1@b"))).containsExactly(
                new Routed<>(new Delete<>("e1", "e1@a"), true),
                new Routed<>(new Upsert<>("e1", "e1@b"), false));
    }

    @Test
    void deleteIsForwarded() throws Exception {
        assertThat(rules.route("e1@a", new Delete<>("e1", "e1@a")))
                .containsExactly(new Routed<>(new Delete<>("e1", "e1@a"), false));
    }
}
