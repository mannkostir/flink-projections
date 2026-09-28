package io.github.mannkostir.projections;

import java.util.List;

import org.apache.flink.api.java.functions.KeySelector;

final class SlotParentKey implements KeySelector<SlotChange, String> {
    private final String level;
    private final List<String> slotNames;
    private final List<KeySelector<Object, String>> parentKeys;

    SlotParentKey(String level, List<String> slotNames, List<KeySelector<Object, String>> parentKeys) {
        this.level = level;
        this.slotNames = List.copyOf(slotNames);
        this.parentKeys = List.copyOf(parentKeys);
    }

    @Override
    public String getKey(SlotChange slotChange) throws Exception {
        String parentKey = parentKeys.get(slotChange.slot()).getKey(slotChange.change().value());
        if (parentKey == null) {
            throw new IllegalStateException("Nest '" + level + "' slot '" + slotNames.get(slotChange.slot())
                    + "': parent key selector returned null for child id '" + slotChange.change().id() + "'");
        }
        return parentKey;
    }
}
