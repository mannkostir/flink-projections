package io.github.mannkostir.projections.kafka;

import java.util.Objects;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;

import io.github.mannkostir.projections.Change;

public abstract sealed class DeleteDetection<T> permits FlagDeleteDetection {
    DeleteDetection() {
    }

    public static <T> DeleteDetection<T> flag(FilterFunction<T> isDeleted) {
        return new FlagDeleteDetection<>(Objects.requireNonNull(isDeleted, "isDeleted"));
    }

    abstract DataStream<Change<T>> toChanges(String name, DataStream<T> values, KeySelector<T, String> id, TypeInformation<T> type);
}
