package io.github.mannkostir.projections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

class NestRulesTest {
    private static final ChildSlot<String> SKILLS = new ChildSlot<>("candidate", "skills", 0);
    private static final ChildSlot<String> JOBS = new ChildSlot<>("candidate", "jobs", 1);

    private final NestRules<String, String> rules = new NestRules<>("candidate", 2,
            (parent, children) -> parent + "|" + children.get(SKILLS) + "|" + children.get(JOBS));
    private final InMemoryLevelState<String, String> state = new InMemoryLevelState<>(2);
    private final List<Change<String>> out = new ArrayList<>();

    @Test
    void childUpsertWithoutParentIsStoredSilently() throws Exception {
        rules.onChild("c1", skill(new Upsert<>("s1", "java")), state, out::add);

        assertThat(out).isEmpty();
        assertThat(state.children(0)).containsExactly("java");
    }

    @Test
    void childUpsertWithParentEmitsDocument() throws Exception {
        rules.onParent("c1", new Upsert<>("c1", "alice"), state, out::add);
        rules.onChild("c1", skill(new Upsert<>("s1", "java")), state, out::add);

        assertThat(out).last().isEqualTo(new Upsert<>("c1", "alice|[java]|[]"));
    }

    @Test
    void storedChildDeleteWithParentEmitsDocumentWithoutIt() throws Exception {
        rules.onParent("c1", new Upsert<>("c1", "alice"), state, out::add);
        rules.onChild("c1", skill(new Upsert<>("s1", "java")), state, out::add);
        rules.onChild("c1", skill(new Delete<>("s1", "java")), state, out::add);

        assertThat(out).last().isEqualTo(new Upsert<>("c1", "alice|[]|[]"));
    }

    @Test
    void unknownChildDeleteEmitsNothing() throws Exception {
        rules.onParent("c1", new Upsert<>("c1", "alice"), state, out::add);
        out.clear();
        rules.onChild("c1", skill(new Delete<>("s9", "rust")), state, out::add);

        assertThat(out).isEmpty();
    }

    @Test
    void storedChildDeleteWithoutParentRemovesSilently() throws Exception {
        rules.onChild("c1", skill(new Upsert<>("s1", "java")), state, out::add);
        rules.onChild("c1", skill(new Delete<>("s1", "java")), state, out::add);

        assertThat(out).isEmpty();
        assertThat(state.childCount()).isZero();
    }

    @Test
    void parentUpsertEmitsDocumentWithStoredChildren() throws Exception {
        rules.onChild("c1", job(new Upsert<>("j1", "acme")), state, out::add);
        rules.onParent("c1", new Upsert<>("c1", "alice"), state, out::add);

        assertThat(out).containsExactly(new Upsert<>("c1", "alice|[]|[acme]"));
    }

    @Test
    void parentDeleteEmitsDeleteWithLastDocumentAndClearsEverything() throws Exception {
        rules.onParent("c1", new Upsert<>("c1", "alice"), state, out::add);
        rules.onChild("c1", skill(new Upsert<>("s1", "java")), state, out::add);
        rules.onParent("c1", new Delete<>("c1", "alice"), state, out::add);

        assertThat(out).last().isEqualTo(new Delete<>("c1", "alice|[java]|[]"));
        assertThat(state.parent()).isNull();
        assertThat(state.lastDoc()).isNull();
        assertThat(state.childCount()).isZero();
    }

    @Test
    void parentDeleteWithoutParentClearsChildrenSilently() throws Exception {
        rules.onChild("c1", skill(new Upsert<>("s1", "java")), state, out::add);
        rules.onParent("c1", new Delete<>("c1", "alice"), state, out::add);

        assertThat(out).isEmpty();
        assertThat(state.childCount()).isZero();
    }

    @Test
    void childrenArePassedSortedById() throws Exception {
        rules.onChild("c1", skill(new Upsert<>("s2", "scala")), state, out::add);
        rules.onChild("c1", skill(new Upsert<>("s1", "java")), state, out::add);
        rules.onParent("c1", new Upsert<>("c1", "alice"), state, out::add);

        assertThat(out).containsExactly(new Upsert<>("c1", "alice|[java, scala]|[]"));
    }

    @Test
    void orphanTimeoutClearsChildrenWhenParentAbsent() throws Exception {
        rules.onChild("c1", skill(new Upsert<>("s1", "java")), state, out::add);
        rules.onOrphanTimeout(state);

        assertThat(state.childCount()).isZero();
    }

    @Test
    void orphanTimeoutKeepsChildrenWhenParentPresent() throws Exception {
        rules.onChild("c1", skill(new Upsert<>("s1", "java")), state, out::add);
        rules.onParent("c1", new Upsert<>("c1", "alice"), state, out::add);
        rules.onOrphanTimeout(state);

        assertThat(state.childCount()).isEqualTo(1);
    }

    @Test
    void childrenRejectSlotOfAnotherLevel() {
        Children children = new Children("candidate", List.of(List.of()));

        assertThatThrownBy(() -> children.get(new ChildSlot<String>("experience", "projects", 0)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("experience");
    }

    @Test
    void assemblerFailurePropagates() {
        NestRules<String, String> failing = new NestRules<>("candidate", 2, (parent, children) -> {
            throw new IllegalStateException("broken assembler");
        });

        assertThatThrownBy(() -> failing.onParent("c1", new Upsert<>("c1", "alice"), state, out::add))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("broken assembler");
    }

    @Test
    void childrenListIsUnmodifiable() throws Exception {
        NestRules<String, String> mutating = new NestRules<>("candidate", 2, (parent, children) -> {
            children.get(SKILLS).add("injected");
            return parent;
        });
        rules.onChild("c1", skill(new Upsert<>("s1", "java")), state, out::add);

        assertThatThrownBy(() -> mutating.onParent("c1", new Upsert<>("c1", "alice"), state, out::add))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThat(state.children(0)).containsExactly("java");
    }

    @Test
    void replayedUpsertEmitsIdenticalDocument() throws Exception {
        rules.onParent("c1", new Upsert<>("c1", "alice"), state, out::add);
        rules.onChild("c1", skill(new Upsert<>("s1", "java")), state, out::add);
        rules.onChild("c1", skill(new Upsert<>("s1", "java")), state, out::add);

        assertThat(out.get(2)).isEqualTo(out.get(1));
    }

    private static SlotChange skill(Change<String> change) {
        return new SlotChange(0, change);
    }

    private static SlotChange job(Change<String> change) {
        return new SlotChange(1, change);
    }
}
