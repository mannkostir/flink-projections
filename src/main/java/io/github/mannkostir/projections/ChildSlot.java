package io.github.mannkostir.projections;

import java.io.Serializable;

public final class ChildSlot<C> implements Serializable {
    private final String level;
    private final String name;
    private final int index;

    ChildSlot(String level, String name, int index) {
        this.level = level;
        this.name = name;
        this.index = index;
    }

    String level() {
        return level;
    }

    String name() {
        return name;
    }

    int index() {
        return index;
    }

    @Override
    public String toString() {
        return level + "." + name;
    }
}
