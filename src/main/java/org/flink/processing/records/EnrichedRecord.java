package org.flink.processing.records;

import org.flink.processing.records.interfaces.DeletableRecord;
import org.flink.processing.records.interfaces.IdentifiableRecord;
import org.flink.processing.records.interfaces.KeyedRecord;

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
