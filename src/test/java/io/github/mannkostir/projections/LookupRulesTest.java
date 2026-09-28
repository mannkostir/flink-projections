package io.github.mannkostir.projections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

class LookupRulesTest {
    private final InMemoryLookupState<String, String> state = new InMemoryLookupState<>();
    private final List<Change<String>> out = new ArrayList<>();

    @Test
    void entityUpsertWithoutDimensionEmitsWithNull() throws Exception {
        leftJoin().onEntity(entity(new Upsert<>("e1", "dev")), state, out::add);

        assertThat(out).containsExactly(new Upsert<>("e1", "dev@null"));
    }

    @Test
    void entityUpsertWithDimensionEmitsEnriched() throws Exception {
        leftJoin().onDimension(new Upsert<>("k1", "acme"), state, out::add);
        leftJoin().onEntity(entity(new Upsert<>("e1", "dev")), state, out::add);

        assertThat(out).containsExactly(new Upsert<>("e1", "dev@acme"));
    }

    @Test
    void storedEntityDeleteEmitsDeleteWithStoredEntity() throws Exception {
        leftJoin().onDimension(new Upsert<>("k1", "acme"), state, out::add);
        leftJoin().onEntity(entity(new Upsert<>("e1", "dev")), state, out::add);
        leftJoin().onEntity(entity(new Delete<>("e1", "dev")), state, out::add);

        assertThat(out).last().isEqualTo(new Delete<>("e1", "dev@acme"));
        assertThat(state.entities()).isEmpty();
    }

    @Test
    void unknownEntityDeleteEmitsNothing() throws Exception {
        leftJoin().onEntity(entity(new Delete<>("e9", "ghost")), state, out::add);

        assertThat(out).isEmpty();
    }

    @Test
    void dimensionUpsertReEmitsEveryEntity() throws Exception {
        leftJoin().onEntity(entity(new Upsert<>("e2", "qa")), state, out::add);
        leftJoin().onEntity(entity(new Upsert<>("e1", "dev")), state, out::add);
        out.clear();
        leftJoin().onDimension(new Upsert<>("k1", "acme"), state, out::add);

        assertThat(out).containsExactly(new Upsert<>("e1", "dev@acme"), new Upsert<>("e2", "qa@acme"));
    }

    @Test
    void dimensionDeleteReEmitsEntitiesWithNullAndKeepsThem() throws Exception {
        leftJoin().onDimension(new Upsert<>("k1", "acme"), state, out::add);
        leftJoin().onEntity(entity(new Upsert<>("e1", "dev")), state, out::add);
        out.clear();
        leftJoin().onDimension(new Delete<>("k1", "acme"), state, out::add);

        assertThat(out).containsExactly(new Upsert<>("e1", "dev@null"));
        assertThat(state.entities()).containsKey("e1");
    }

    @Test
    void requireMatchHoldsEntityUntilDimensionArrives() throws Exception {
        innerJoin().onEntity(entity(new Upsert<>("e1", "dev")), state, out::add);
        assertThat(out).isEmpty();

        innerJoin().onDimension(new Upsert<>("k1", "acme"), state, out::add);
        assertThat(out).containsExactly(new Upsert<>("e1", "dev@acme"));
    }

    @Test
    void requireMatchDeletesEntitiesWhenDimensionDeleted() throws Exception {
        innerJoin().onDimension(new Upsert<>("k1", "acme"), state, out::add);
        innerJoin().onEntity(entity(new Upsert<>("e1", "dev")), state, out::add);
        out.clear();
        innerJoin().onDimension(new Delete<>("k1", "acme"), state, out::add);

        assertThat(out).containsExactly(new Delete<>("e1", "dev@acme"));
        assertThat(state.entities()).containsKey("e1");
    }

    @Test
    void requireMatchEntityDeleteWithoutDimensionEmitsNothing() throws Exception {
        innerJoin().onEntity(entity(new Upsert<>("e1", "dev")), state, out::add);
        innerJoin().onEntity(entity(new Delete<>("e1", "dev")), state, out::add);

        assertThat(out).isEmpty();
        assertThat(state.entities()).isEmpty();
    }

    @Test
    void requireMatchEmitsEntityUpsertWhenDimensionPresent() throws Exception {
        innerJoin().onDimension(new Upsert<>("k1", "acme"), state, out::add);
        innerJoin().onEntity(entity(new Upsert<>("e1", "dev")), state, out::add);

        assertThat(out).containsExactly(new Upsert<>("e1", "dev@acme"));
    }

    @Test
    void requireMatchEmitsEntityDeleteWhenDimensionPresent() throws Exception {
        innerJoin().onDimension(new Upsert<>("k1", "acme"), state, out::add);
        innerJoin().onEntity(entity(new Upsert<>("e1", "dev")), state, out::add);
        out.clear();
        innerJoin().onEntity(entity(new Delete<>("e1", "dev")), state, out::add);

        assertThat(out).containsExactly(new Delete<>("e1", "dev@acme"));
    }

    @Test
    void relocatedEntityIsRemovedSilently() throws Exception {
        leftJoin().onDimension(new Upsert<>("k1", "acme"), state, out::add);
        leftJoin().onEntity(entity(new Upsert<>("e1", "dev")), state, out::add);
        out.clear();
        leftJoin().onEntity(new LookupEntity<>(new Delete<>("e1", "dev"), true), state, out::add);

        assertThat(out).isEmpty();
        assertThat(state.entities()).isEmpty();
    }

    @Test
    void enricherFailurePropagates() {
        LookupRules<String, String, String> failing = new LookupRules<>((entity, dimension) -> {
            throw new IllegalStateException("broken enricher");
        }, false);

        assertThatThrownBy(() -> failing.onEntity(entity(new Upsert<>("e1", "dev")), state, out::add))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("broken enricher");
    }

    private static LookupEntity<String> entity(Change<String> change) {
        return new LookupEntity<>(change, false);
    }

    private static LookupRules<String, String, String> leftJoin() {
        return new LookupRules<>((entity, dimension) -> entity + "@" + dimension, false);
    }

    private static LookupRules<String, String, String> innerJoin() {
        return new LookupRules<>((entity, dimension) -> entity + "@" + dimension, true);
    }
}
