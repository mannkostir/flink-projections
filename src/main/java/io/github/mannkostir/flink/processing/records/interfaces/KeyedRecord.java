package io.github.mannkostir.flink.processing.records.interfaces;

public interface KeyedRecord extends CommonRecord {
    String getKey ();
}
