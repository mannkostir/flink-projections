package io.github.mannkostir.projections.elasticsearch;

import java.time.Duration;

interface Sleeper {
    Sleeper THREAD = delay -> Thread.sleep(delay.toMillis());

    void sleep(Duration delay) throws InterruptedException;
}
