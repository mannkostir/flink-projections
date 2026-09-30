package io.github.mannkostir.projections.elasticsearch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import io.github.mannkostir.projections.Change;
import io.github.mannkostir.projections.Changes;
import io.github.mannkostir.projections.ProjectionConfigurationException;
import io.github.mannkostir.projections.Upsert;

class ElasticsearchContractNamesTest {
    private static final ElasticsearchSinkOptions OPTIONS = ElasticsearchSinkOptions.builder()
            .hosts("http://localhost:9200").index("docs").build();

    private static DataStream<Change<String>> changes(StreamExecutionEnvironment env) {
        return env.fromData(List.of(new Upsert<>("a", "{}")), Changes.typeInfo(Types.STRING));
    }

    @Test
    void sinkUid() {
        assertThat(ElasticsearchContractNames.sinkUid("candidate-docs")).isEqualTo("elasticsearch_sink_candidate-docs");
    }

    @Test
    void sinkIsNamedElasticsearchSink() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSink<Change<String>> sink = ElasticsearchChanges.to("docs", changes(env), OPTIONS, new SimpleStringSchema());

        assertThat(sink.getTransformation().getUid()).isEqualTo("elasticsearch_sink_docs");
        assertThat(sink.getTransformation().getName()).isEqualTo("elasticsearch_sink_docs");
    }

    @Test
    void rejectsInvalidSinkName() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Change<String>> changes = changes(env);

        assertThatThrownBy(() -> ElasticsearchChanges.to("Docs", changes, OPTIONS, new SimpleStringSchema()))
                .isInstanceOf(ProjectionConfigurationException.class)
                .hasMessageContaining("'Docs'");
    }
}
