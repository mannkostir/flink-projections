package io.github.mannkostir.projections.processing.records;

import java.util.ArrayList;

import io.github.mannkostir.projections.processing.records.interfaces.CommonRecord;

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
