package io.github.mannkostir.flink.processing.records.interfaces;

public interface IdentifiableRecord extends CommonRecord {
    String recordId ();
}
