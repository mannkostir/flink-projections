package io.github.mannkostir.projections.processing.jobs.interfaces;

import io.github.mannkostir.projections.processing.records.interfaces.CommonRecord;

public interface ICreateTransformedGroupedRecords<In extends CommonRecord, Out extends CommonRecord, OutGroup extends CommonRecord>
        extends ITransformRecords<In, Out>, ICreateGroupRecords<Out, OutGroup> {
}
