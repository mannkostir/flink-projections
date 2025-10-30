package org.flink.processing.jobs.interfaces;

import java.io.Serializable;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.flink.processing.records.EnrichedRecord;

public interface ICreateEnrichedRecords<In1, In2, Out extends EnrichedRecord> extends Serializable {
    Out createEnrichedRecord (In1 source1, In2 source2);

    TypeInformation<Out> enrichedRecordTypeInformation ();
}
