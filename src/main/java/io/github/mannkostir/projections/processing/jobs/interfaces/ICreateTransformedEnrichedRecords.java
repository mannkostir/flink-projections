package io.github.mannkostir.projections.processing.jobs.interfaces;

import io.github.mannkostir.projections.processing.records.EnrichedRecord;
import io.github.mannkostir.projections.processing.records.SinkRecord;

public interface ICreateTransformedEnrichedRecords<In1, In2, Enriched extends EnrichedRecord, Sink extends SinkRecord<?>>
        extends ICreateEnrichedRecords<In1, In2, Enriched>, ITransformRecords<Enriched, Sink> {
}
