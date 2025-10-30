package org.flink.processing.records;

public class EnvelopeRecord<Payload extends Object> {
    private final Payload payload;

    public EnvelopeRecord (Payload payload) {
        this.payload = payload;
    }

    public Payload getPayload () {
        return this.payload;
    }
}
