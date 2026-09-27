package io.github.mannkostir.projections.processing.records;

import io.github.mannkostir.projections.processing.records.interfaces.CommonRecord;

public class EnvelopeRecord<Payload extends Object> implements CommonRecord {
    private final Payload payload;

    public EnvelopeRecord (Payload payload) {
        this.payload = payload;
    }

    public Payload getPayload () {
        return this.payload;
    }
}
