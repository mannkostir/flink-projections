package org.flink.processing.jobs.interfaces;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.flink.processing.records.RecordsGroup;
import org.flink.processing.records.interfaces.CommonRecord;

interface ICreateGroupRecords<In extends CommonRecord, Out extends CommonRecord> {
    Out createGroupRecord (RecordsGroup<In> records);

    TypeInformation<Out> groupedRecordTypeInformation ();
}
