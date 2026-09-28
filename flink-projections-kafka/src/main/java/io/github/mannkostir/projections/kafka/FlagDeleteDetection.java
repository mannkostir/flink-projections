package io.github.mannkostir.projections.kafka;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;

import io.github.mannkostir.projections.Change;
import io.github.mannkostir.projections.Changes;

final class FlagDeleteDetection<T> extends DeleteDetection<T> {
    private final FilterFunction<T> isDeleted;

    FlagDeleteDetection(FilterFunction<T> isDeleted) {
        this.isDeleted = isDeleted;
    }

    @Override
    DataStream<Change<T>> toChanges(String name, DataStream<T> values, KeySelector<T, String> id, TypeInformation<T> type) {
        return Changes.from(name, values, id, isDeleted, type);
    }
}
