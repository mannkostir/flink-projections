package io.github.mannkostir.projections.kafka;

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

class KafkaContractNamesTest {
    private static final KafkaSourceOptions SOURCE = KafkaSourceOptions.builder()
            .bootstrapServers("localhost:9092").topic("rows").groupId("test").build();
    private static final KafkaSinkOptions SINK = KafkaSinkOptions.builder()
            .bootstrapServers("localhost:9092").topic("docs").build();

    @Test
    void operatorUids() {
        assertThat(KafkaContractNames.sourceUid("candidates")).isEqualTo("kafka_source_candidates");
        assertThat(KafkaContractNames.sinkUid("candidate-docs")).isEqualTo("kafka_sink_candidate-docs");
    }

    @Test
    void sourceIsNamedKafkaSourceThenChanges() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Change<String>> changes = KafkaChanges.from(
                "rows", env, SOURCE, new SimpleStringSchema(), row -> row, DeleteDetection.flag(row -> false), Types.STRING);

        assertThat(changes.getTransformation().getUid()).isEqualTo("changes_rows");
        assertThat(changes.getTransformation().getInputs().get(0).getUid()).isEqualTo("kafka_source_rows");
    }

    @Test
    void sinkIsNamedKafkaSink() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Change<String>> changes = env.fromData(List.of(new Upsert<>("a", "x")), Changes.typeInfo(Types.STRING));

        DataStreamSink<Change<String>> sink = KafkaChanges.to("docs", changes, SINK, new SimpleStringSchema());

        assertThat(sink.getTransformation().getUid()).isEqualTo("kafka_sink_docs");
        assertThat(sink.getTransformation().getName()).isEqualTo("kafka_sink_docs");
    }

    @Test
    void rejectsInvalidSourceNameBeforeAddingOperators() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        assertThatThrownBy(() -> KafkaChanges.from(
                "Rows", env, SOURCE, new SimpleStringSchema(), row -> row, DeleteDetection.flag(row -> false), Types.STRING))
                .isInstanceOf(ProjectionConfigurationException.class)
                .hasMessageContaining("'Rows'");
        assertThat(env.getTransformations()).isEmpty();
    }

    @Test
    void rejectsInvalidSinkName() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Change<String>> changes = env.fromData(List.of(new Upsert<>("a", "x")), Changes.typeInfo(Types.STRING));

        assertThatThrownBy(() -> KafkaChanges.to("docs_out", changes, SINK, new SimpleStringSchema()))
                .isInstanceOf(ProjectionConfigurationException.class)
                .hasMessageContaining("'docs_out'");
    }
}
