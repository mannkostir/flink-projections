package io.github.mannkostir.projections;

public record Upsert<T>(String id, T value) implements Change<T> {
    public Upsert {
        ChangeInvariants.requireValid(id, value);
    }
}
