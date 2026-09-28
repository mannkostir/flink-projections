package io.github.mannkostir.projections;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

final class InMemoryLevelState<P, O> implements LevelState<P, O> {
    private final List<TreeMap<String, Object>> children = new ArrayList<>();
    private P parent;
    private O lastDoc;

    InMemoryLevelState(int slotCount) {
        for (int slot = 0; slot < slotCount; slot++) {
            children.add(new TreeMap<>());
        }
    }

    @Override
    public P parent() {
        return parent;
    }

    @Override
    public void putParent(P value) {
        parent = value;
    }

    @Override
    public boolean hasChild(int slot, String childId) {
        return children.get(slot).containsKey(childId);
    }

    @Override
    public void putChild(int slot, String childId, Object child) {
        children.get(slot).put(childId, child);
    }

    @Override
    public void removeChild(int slot, String childId) {
        children.get(slot).remove(childId);
    }

    @Override
    public List<Object> children(int slot) {
        return List.copyOf(children.get(slot).values());
    }

    @Override
    public O lastDoc() {
        return lastDoc;
    }

    @Override
    public void putLastDoc(O doc) {
        lastDoc = doc;
    }

    @Override
    public void clearChildren() {
        children.forEach(TreeMap::clear);
    }

    @Override
    public void clearAll() {
        parent = null;
        lastDoc = null;
        clearChildren();
    }

    int childCount() {
        return children.stream().mapToInt(TreeMap::size).sum();
    }
}
