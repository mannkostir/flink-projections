package io.github.mannkostir.projections.processing.jobs.interfaces;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import io.github.mannkostir.projections.processing.records.RecordsGroup;
import io.github.mannkostir.projections.processing.records.interfaces.CommonRecord;

interface ICreateGroupRecords<In extends CommonRecord, Out extends CommonRecord> {
    Out createGroupRecord (RecordsGroup<In> records);

    TypeInformation<Out> groupedRecordTypeInformation ();
}
