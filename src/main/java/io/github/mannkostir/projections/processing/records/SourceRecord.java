package io.github.mannkostir.projections.processing.records;

import io.github.mannkostir.projections.processing.records.interfaces.DeletableRecord;
import io.github.mannkostir.projections.processing.records.interfaces.IdentifiableRecord;
import io.github.mannkostir.projections.processing.records.interfaces.KeyedRecord;

public abstract class SourceRecord<Payload extends Object>
        extends EnvelopeRecord<Payload>
        implements IdentifiableRecord, DeletableRecord, KeyedRecord {
    public SourceRecord (Payload payload) {
        super(payload);
        this.validate();
    }

    private void validate () {
        var isValid = this.recordId() != null && !this.recordId().isBlank();

        if (!isValid) {
            throw new RuntimeException("Invalid recordId for source record: " +
                                       this.getPayload().getClass().getName() + " " +
                                       this.recordId());
        }
    }

    public String recordId () {
        return "";
    }

    @Override
    public String getKey () {
        return this.recordId();
    }

    public Boolean isDeleted () {
        return false;
    }
}
