package io.github.mannkostir.projections.processing.jobs;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;

public class DataStreamWithKey<T> {
    private final DataStream<T> sourceStream;
    private final KeySelector<T, String> keySelector;

    public DataStreamWithKey (DataStream<T> sourceStream, KeySelector<T, String> keySelector) {
        this.sourceStream = sourceStream;
        this.keySelector = keySelector;
    }

    public DataStream<T> stream () {
        return this.sourceStream;
    }

    public KeySelector<T, String> keySelector () {
        return this.keySelector;
    }

    public Class<T> sourceClass () {
        return this.sourceStream.getType().getTypeClass();
    }
}
