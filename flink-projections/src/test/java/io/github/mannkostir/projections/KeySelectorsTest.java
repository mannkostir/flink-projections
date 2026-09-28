package io.github.mannkostir.projections;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.apache.flink.api.java.functions.KeySelector;
import org.junit.jupiter.api.Test;

class KeySelectorsTest {
    @Test
    void nullParentKeyFailsNamingLevelSlotAndChild() {
        KeySelector<Object, String> noParent = value -> null;
        SlotParentKey key = new SlotParentKey("candidate", List.of("skills"), List.of(noParent));

        assertThatThrownBy(() -> key.getKey(new SlotChange(0, new Upsert<>("s1", "java"))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("'candidate'")
                .hasMessageContaining("'skills'")
                .hasMessageContaining("'s1'");
    }

    @Test
    void nullLookupKeyFailsNamingLookupAndEntity() {
        LookupEntityKey<String> key = new LookupEntityKey<>("company", value -> null);

        assertThatThrownBy(() -> key.getKey(new LookupEntity<>(new Upsert<>("e1", "dev"), false)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("'company'")
                .hasMessageContaining("'e1'");
    }
}
