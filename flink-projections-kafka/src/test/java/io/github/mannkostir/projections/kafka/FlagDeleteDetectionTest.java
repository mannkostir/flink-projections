package io.github.mannkostir.projections.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.github.mannkostir.projections.Change;
import io.github.mannkostir.projections.Delete;
import io.github.mannkostir.projections.Upsert;

class FlagDeleteDetectionTest {
    @RegisterExtension
    static final MiniClusterExtension CLUSTER = new MiniClusterExtension();

    @Test
    void flaggedRecordsBecomeDeletesAndOthersUpserts() throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(PipelineOptions.GENERIC_TYPES, false);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        DataStream<String> rows = env.fromData(Types.STRING, "a:live", "b:gone");
        DeleteDetection<String> deletes = DeleteDetection.flag(row -> row.endsWith("gone"));

        DataStream<Change<String>> changes = deletes.toChanges("rows", rows, row -> row.split(":")[0], Types.STRING);

        assertThat(changes.executeAndCollect(10)).containsExactlyInAnyOrder(
                new Upsert<>("a", "a:live"),
                new Delete<>("b", "b:gone"));
    }
}
