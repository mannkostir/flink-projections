package io.github.mannkostir.projections;

import org.apache.flink.api.java.functions.KeySelector;

final class LookupEntityKey<E> implements KeySelector<LookupEntity<E>, String> {
    private final String name;
    private final KeySelector<E, String> lookupKey;

    LookupEntityKey(String name, KeySelector<E, String> lookupKey) {
        this.name = name;
        this.lookupKey = lookupKey;
    }

    @Override
    public String getKey(LookupEntity<E> entity) throws Exception {
        String key = lookupKey.getKey(entity.change().value());
        if (key == null) {
            throw new IllegalStateException("Lookup '" + name + "': lookup key selector returned null for entity id '"
                    + entity.change().id() + "'");
        }
        return key;
    }
}
