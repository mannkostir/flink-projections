package io.github.mannkostir.projections.processing.workers.base;

public abstract class Worker<Out> {
    public abstract Out run (String processName);
}
