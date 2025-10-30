package org.flink.processing.jobs.interfaces;

import org.flink.processing.records.interfaces.CommonRecord;

public interface ICreateTransformedGroupedRecords<In extends CommonRecord, Out extends CommonRecord, OutGroup extends CommonRecord>
        extends ITransformRecords<In, Out>, ICreateGroupRecords<Out, OutGroup> {
}
