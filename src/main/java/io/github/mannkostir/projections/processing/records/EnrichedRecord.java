package io.github.mannkostir.projections.processing.records;

import io.github.mannkostir.projections.processing.records.interfaces.DeletableRecord;
import io.github.mannkostir.projections.processing.records.interfaces.IdentifiableRecord;
import io.github.mannkostir.projections.processing.records.interfaces.KeyedRecord;

public abstract class EnrichedRecord
        implements KeyedRecord, IdentifiableRecord, DeletableRecord {
    public EnrichedRecord () {
    }

    public Boolean isReady () {
        return this.recordId() != null && this.getKey() != null;
    }

    @Override
    public Boolean isDeleted () {
        return false;
    }
}
