package io.github.mannkostir.projections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class ChangesTest {
    @RegisterExtension
    static final MiniClusterExtension CLUSTER = new MiniClusterExtension();

    @Test
    void mapsDeletedRowsToDeleteAndOthersToUpsert() throws Exception {
        StreamExecutionEnvironment env = environment();
        DataStream<String> rows = env.fromData(Types.STRING, "a:live", "b:gone");

        DataStream<Change<String>> changes = Changes.from(
                "rows", rows, row -> row.split(":")[0], row -> row.endsWith("gone"), Types.STRING);

        assertThat(changes.executeAndCollect(10)).containsExactlyInAnyOrder(
                new Upsert<>("a", "a:live"),
                new Delete<>("b", "b:gone"));
    }

    @Test
    void rejectsInvalidName() {
        StreamExecutionEnvironment env = environment();
        DataStream<String> rows = env.fromData(Types.STRING, "a:live");

        assertThatThrownBy(() -> Changes.from("Rows", rows, row -> row, row -> false, Types.STRING))
                .isInstanceOf(ProjectionConfigurationException.class)
                .hasMessageContaining("'Rows'");
    }

    @Test
    void usesNameInUid() {
        StreamExecutionEnvironment env = environment();
        DataStream<String> rows = env.fromData(Types.STRING, "a:live");

        DataStream<Change<String>> changes = Changes.from("rows", rows, row -> row, row -> false, Types.STRING);

        assertThat(changes.getTransformation().getUid()).isEqualTo("changes_rows");
    }

    static StreamExecutionEnvironment environment() {
        Configuration configuration = new Configuration();
        configuration.set(PipelineOptions.GENERIC_TYPES, false);
        return StreamExecutionEnvironment.getExecutionEnvironment(configuration);
    }
}
