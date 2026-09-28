package io.github.mannkostir.projections.kafka;

public final class TombstoneNotSupportedException extends RuntimeException {
    TombstoneNotSupportedException(String topic, int partition, long offset) {
        super("Tombstone at " + topic + "-" + partition + "@" + offset
                + ": DeleteDetection.flag expects soft-delete records that carry the entity's last value, not null-value tombstones");
    }
}
