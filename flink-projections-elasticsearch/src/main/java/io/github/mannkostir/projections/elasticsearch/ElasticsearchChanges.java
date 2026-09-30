package io.github.mannkostir.projections.elasticsearch;

import java.util.Objects;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;

import io.github.mannkostir.projections.Change;

public final class ElasticsearchChanges {
    private ElasticsearchChanges() {
    }

    public static <T> DataStreamSink<Change<T>> to(
            String name,
            DataStream<Change<T>> changes,
            ElasticsearchSinkOptions options,
            SerializationSchema<T> format) {
        ElasticsearchNames.requireValid(name, "Elasticsearch sink name");
        Objects.requireNonNull(changes, "changes");
        Objects.requireNonNull(options, "options");
        Objects.requireNonNull(format, "format");
        String uid = ElasticsearchContractNames.sinkUid(name);
        return changes
                .keyBy(Change::id, Types.STRING)
                .sinkTo(new ChangeSink<>(options, format, ElasticsearchBulkClient::open))
                .uid(uid)
                .name(uid);
    }
}
