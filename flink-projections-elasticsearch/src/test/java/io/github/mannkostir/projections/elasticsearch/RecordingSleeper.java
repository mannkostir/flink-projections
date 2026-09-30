package io.github.mannkostir.projections.elasticsearch;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

final class RecordingSleeper implements Sleeper {
    private final List<Duration> delays = new ArrayList<>();

    @Override
    public void sleep(Duration delay) {
        delays.add(delay);
    }

    List<Duration> delays() {
        return List.copyOf(delays);
    }
}
