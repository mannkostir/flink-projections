package io.github.mannkostir.projections;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

final class NestRules<P, O> implements Serializable {
    private final String level;
    private final int slotCount;
    private final Assembler<P, O> assembler;

    NestRules(String level, int slotCount, Assembler<P, O> assembler) {
        this.level = level;
        this.slotCount = slotCount;
        this.assembler = assembler;
    }

    void onParent(String parentId, Change<P> change, LevelState<P, O> state, Consumer<Change<O>> out) throws Exception {
        if (change instanceof Upsert<P> upsert) {
            state.putParent(upsert.value());
            emitAssembled(parentId, state, out);
            return;
        }
        deleteParent(parentId, state, out);
    }

    void onChild(String parentId, SlotChange slotChange, LevelState<P, O> state, Consumer<Change<O>> out) throws Exception {
        Change<?> change = slotChange.change();
        if (change instanceof Upsert<?>) {
            state.putChild(slotChange.slot(), change.id(), change.value());
            emitIfParentPresent(parentId, state, out);
            return;
        }
        if (state.hasChild(slotChange.slot(), change.id())) {
            state.removeChild(slotChange.slot(), change.id());
            emitIfParentPresent(parentId, state, out);
        }
    }

    void onOrphanTimeout(LevelState<P, O> state) throws Exception {
        if (state.parent() == null) {
            state.clearChildren();
        }
    }

    private void deleteParent(String parentId, LevelState<P, O> state, Consumer<Change<O>> out) throws Exception {
        if (state.parent() == null) {
            state.clearChildren();
            return;
        }
        O lastDoc = state.lastDoc();
        state.clearAll();
        out.accept(new Delete<>(parentId, lastDoc));
    }

    private void emitIfParentPresent(String parentId, LevelState<P, O> state, Consumer<Change<O>> out) throws Exception {
        if (state.parent() != null) {
            emitAssembled(parentId, state, out);
        }
    }

    private void emitAssembled(String parentId, LevelState<P, O> state, Consumer<Change<O>> out) throws Exception {
        O doc = assembler.assemble(state.parent(), children(state));
        state.putLastDoc(doc);
        out.accept(new Upsert<>(parentId, doc));
    }

    private Children children(LevelState<P, O> state) throws Exception {
        List<List<Object>> slotValues = new ArrayList<>(slotCount);
        for (int slot = 0; slot < slotCount; slot++) {
            slotValues.add(List.copyOf(state.children(slot)));
        }
        return new Children(level, List.copyOf(slotValues));
    }
}
