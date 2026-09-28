package io.github.mannkostir.projections;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.ValueState;

final class FlinkLevelState<P, O> implements LevelState<P, O> {
    private final ValueState<P> parent;
    private final List<MapState<String, Object>> children;
    private final ValueState<O> lastDoc;

    FlinkLevelState(ValueState<P> parent, List<MapState<String, Object>> children, ValueState<O> lastDoc) {
        this.parent = parent;
        this.children = children;
        this.lastDoc = lastDoc;
    }

    @Override
    public P parent() throws Exception {
        return parent.value();
    }

    @Override
    public void putParent(P value) throws Exception {
        parent.update(value);
    }

    @Override
    public boolean hasChild(int slot, String childId) throws Exception {
        return children.get(slot).contains(childId);
    }

    @Override
    public void putChild(int slot, String childId, Object child) throws Exception {
        children.get(slot).put(childId, child);
    }

    @Override
    public void removeChild(int slot, String childId) throws Exception {
        children.get(slot).remove(childId);
    }

    @Override
    public List<Object> children(int slot) throws Exception {
        List<Map.Entry<String, Object>> entries = new ArrayList<>();
        for (Map.Entry<String, Object> entry : children.get(slot).entries()) {
            entries.add(Map.entry(entry.getKey(), entry.getValue()));
        }
        entries.sort(Map.Entry.comparingByKey());
        return entries.stream().map(Map.Entry::getValue).toList();
    }

    @Override
    public O lastDoc() throws Exception {
        return lastDoc.value();
    }

    @Override
    public void putLastDoc(O doc) throws Exception {
        lastDoc.update(doc);
    }

    @Override
    public void clearChildren() throws Exception {
        for (MapState<String, Object> slot : children) {
            slot.clear();
        }
    }

    @Override
    public void clearAll() throws Exception {
        parent.clear();
        lastDoc.clear();
        clearChildren();
    }
}
