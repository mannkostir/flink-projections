package io.github.mannkostir.projections;

public record Delete<T>(String id, T value) implements Change<T> {
    public Delete {
        ChangeInvariants.requireValid(id, value);
    }
}
