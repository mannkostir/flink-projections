package org.flink.processing.records.interfaces;

public interface KeyedRecord extends CommonRecord {
    String getKey ();
}
