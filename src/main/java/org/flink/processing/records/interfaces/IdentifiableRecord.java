package org.flink.processing.records.interfaces;

public interface IdentifiableRecord extends CommonRecord {
    String recordId ();
}
