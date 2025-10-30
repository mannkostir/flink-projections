package org.flink.processing.jobs.interfaces;

import org.flink.processing.records.EnrichedRecord;
import org.flink.processing.records.SinkRecord;

public interface ICreateTransformedEnrichedRecords<In1, In2, Enriched extends EnrichedRecord, Sink extends SinkRecord<?>>
        extends ICreateEnrichedRecords<In1, In2, Enriched>, ITransformRecords<Enriched, Sink> {
}
