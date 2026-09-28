package io.github.mannkostir.projections;

final class ChangeInvariants {
    private ChangeInvariants() {
    }

    static void requireValid(String id, Object value) {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("Change id must be a non-blank string, got: " + id);
        }
        if (value == null) {
            throw new IllegalArgumentException("Change value must not be null for id " + id);
        }
    }
}
