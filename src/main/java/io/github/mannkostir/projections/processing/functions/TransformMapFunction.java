package io.github.mannkostir.projections.processing.functions;

import org.apache.flink.api.common.functions.MapFunction;
import io.github.mannkostir.projections.processing.jobs.interfaces.ITransformRecords;
import io.github.mannkostir.projections.processing.records.interfaces.CommonRecord;

public class TransformMapFunction<In extends CommonRecord, Out extends CommonRecord>
        implements MapFunction<In, Out> {
    private final ITransformRecords<In, Out> recordsFactory;

    public TransformMapFunction (ITransformRecords<In, Out> recordsFactory) {
        this.recordsFactory = recordsFactory;
    }

    public Out map (In value) {
        return recordsFactory.transformRecord(value);
    }
}
