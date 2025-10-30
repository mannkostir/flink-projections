package org.flink.processing.jobs.interfaces;

import java.io.Serializable;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.flink.processing.records.interfaces.CommonRecord;

public interface ITransformRecords<In extends CommonRecord, Out extends CommonRecord>
        extends Serializable {
    Out transformRecord (In data);

    TypeInformation<Out> transformedRecordTypeInformation ();
}

