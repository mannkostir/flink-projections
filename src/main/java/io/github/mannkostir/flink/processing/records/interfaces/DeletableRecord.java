package io.github.mannkostir.flink.processing.records.interfaces;

public interface DeletableRecord extends CommonRecord {
    Boolean isDeleted ();
}
