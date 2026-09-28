package io.github.mannkostir.projections;

import java.util.List;

public final class Children {
    private final String level;
    private final List<List<Object>> slotValues;

    Children(String level, List<List<Object>> slotValues) {
        this.level = level;
        this.slotValues = slotValues;
    }

    @SuppressWarnings("unchecked")
    public <C> List<C> get(ChildSlot<C> slot) {
        if (!slot.level().equals(level)) {
            throw new IllegalArgumentException(
                    "Child slot '" + slot + "' belongs to level '" + slot.level() + "', not to level '" + level + "'");
        }
        return (List<C>) (List<?>) slotValues.get(slot.index());
    }
}
