package io.github.mannkostir.flink.processing.jobs.interfaces;

import io.github.mannkostir.flink.processing.records.interfaces.CommonRecord;

public interface ICreateTransformedGroupedRecords<In extends CommonRecord, Out extends CommonRecord, OutGroup extends CommonRecord>
        extends ITransformRecords<In, Out>, ICreateGroupRecords<Out, OutGroup> {
}
