package io.github.mannkostir.projections;

import java.util.List;

interface LevelState<P, O> {
    P parent() throws Exception;

    void putParent(P parent) throws Exception;

    boolean hasChild(int slot, String childId) throws Exception;

    void putChild(int slot, String childId, Object child) throws Exception;

    void removeChild(int slot, String childId) throws Exception;

    List<Object> children(int slot) throws Exception;

    O lastDoc() throws Exception;

    void putLastDoc(O doc) throws Exception;

    void clearChildren() throws Exception;

    void clearAll() throws Exception;
}
