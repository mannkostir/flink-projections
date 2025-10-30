package org.flink.processing.records;

import org.flink.processing.records.interfaces.KeyedRecord;

public abstract class SinkRecord<Payload extends Object> extends EnvelopeRecord<Payload> implements KeyedRecord
        {
    public SinkRecord (Payload payload) {
        super(payload);
    }

    protected void validate () {
        var isValid = this.getKey() != null && !this.getKey().isBlank();

        if (!isValid) {
            throw new RuntimeException(
                    "Invalid key for sink record: " + this.getPayload().getClass() + " " +
                    this.getKey());
        }
    }
}
