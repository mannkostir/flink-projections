package org.flink.processing.records;

import java.util.ArrayList;

import org.flink.processing.records.interfaces.CommonRecord;

public class RecordsGroup<T extends CommonRecord> {
    public final ArrayList<T> records;
    public final String key;

    public RecordsGroup (String groupKey, ArrayList<T> records) {
        this.records = records;
        this.key = groupKey;
    }

    public RecordsGroup (RecordsGroup<T> group) {
        this.records = group.records;
        this.key = group.key;
    }
}
