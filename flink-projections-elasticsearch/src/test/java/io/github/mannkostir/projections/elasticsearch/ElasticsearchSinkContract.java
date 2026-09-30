package io.github.mannkostir.projections.elasticsearch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.UUID;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

import io.github.mannkostir.projections.Change;
import io.github.mannkostir.projections.Changes;
import io.github.mannkostir.projections.Delete;
import io.github.mannkostir.projections.Upsert;

abstract class ElasticsearchSinkContract {
    @RegisterExtension
    static final MiniClusterExtension CLUSTER = new MiniClusterExtension();

    private final String index = "docs-" + UUID.randomUUID();
    private IndexProbe probe;

    abstract ElasticsearchContainer elasticsearch();

    @BeforeEach
    void openProbe() {
        probe = new IndexProbe(elasticsearch().getHttpHostAddress());
    }

    @AfterEach
    void closeProbe() throws Exception {
        probe.close();
    }

    private void run(List<Change<String>> changes) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ElasticsearchSinkOptions options = ElasticsearchSinkOptions.builder()
                .hosts("http://" + elasticsearch().getHttpHostAddress())
                .index(index)
                .build();
        ElasticsearchChanges.to("docs", env.fromData(changes, Changes.typeInfo(Types.STRING)), options, new SimpleStringSchema());
        env.execute("elasticsearch-sink-it");
    }

    @Test
    void upsertIndexesTheExactDocument() throws Exception {
        run(List.<Change<String>>of(new Upsert<>("a", "{\"name\":\"Zoë\",\"skills\":[\"java\"]}")));

        assertThat(probe.source(index, "a")).contains("{\"name\":\"Zoë\",\"skills\":[\"java\"]}");
    }

    @Test
    void laterUpsertReplacesTheWholeDocument() throws Exception {
        run(List.<Change<String>>of(new Upsert<>("a", "{\"name\":\"alice\",\"age\":30}")));
        run(List.<Change<String>>of(new Upsert<>("a", "{\"name\":\"alicia\"}")));

        assertThat(probe.source(index, "a")).contains("{\"name\":\"alicia\"}");
    }

    @Test
    void deleteRemovesAnIndexedDocument() throws Exception {
        run(List.<Change<String>>of(new Upsert<>("a", "{}")));
        run(List.<Change<String>>of(new Delete<>("a", "{}")));

        assertThat(probe.source(index, "a")).isEmpty();
    }

    @Test
    void upsertThenDeleteInOneBatchLeavesNoDocument() throws Exception {
        probe.createIndex(index, "{}");

        run(List.of(new Upsert<>("a", "{}"), new Delete<>("a", "{}")));

        assertThat(probe.source(index, "a")).isEmpty();
    }

    @Test
    void deleteThenUpsertInOneBatchKeepsTheDocument() throws Exception {
        run(List.of(new Upsert<>("a", "{\"v\":1}"), new Delete<>("a", "{\"v\":1}"), new Upsert<>("a", "{\"v\":2}")));

        assertThat(probe.source(index, "a")).contains("{\"v\":2}");
    }

    @Test
    void replayingTheSameChangesGivesTheSameIndex() throws Exception {
        List<Change<String>> changes = List.of(new Upsert<>("a", "{\"v\":1}"), new Upsert<>("b", "{}"), new Delete<>("b", "{}"));
        run(changes);

        run(changes);

        assertThat(probe.source(index, "a")).contains("{\"v\":1}");
        assertThat(probe.source(index, "b")).isEmpty();
    }

    @Test
    void deletingAMissingDocumentSucceeds() throws Exception {
        probe.createIndex(index, "{}");

        run(List.<Change<String>>of(new Delete<>("never-indexed", "{}")));

        assertThat(probe.source(index, "never-indexed")).isEmpty();
    }

    @Test
    void deleteIntoAMissingIndexSucceeds() {
        assertThatCode(() -> run(List.<Change<String>>of(new Upsert<>("a", "{}"), new Delete<>("a", "{}"))))
                .doesNotThrowAnyException();
    }

    @Test
    void idsWithReservedCharactersRoundTrip() throws Exception {
        run(List.<Change<String>>of(new Upsert<>("tenant/1 ä?#", "{}")));

        assertThat(probe.source(index, "tenant/1 ä?#")).contains("{}");
    }

    @Test
    void mappingConflictFailsTheJobNamingTheDocument() throws Exception {
        probe.createIndex(index, "{\"mappings\":{\"properties\":{\"age\":{\"type\":\"integer\"}}}}");

        assertThatThrownBy(() -> run(List.<Change<String>>of(new Upsert<>("bad-age", "{\"age\":\"thirty\"}"))))
                .rootCause()
                .isInstanceOf(ElasticsearchWriteException.class)
                .hasMessageContaining("document 'bad-age'")
                .hasMessageContaining("check the index mapping");
    }

    @Test
    void nonJsonDocumentFailsTheJob() throws Exception {
        assertThatThrownBy(() -> run(List.<Change<String>>of(new Upsert<>("broken", "not json"))))
                .rootCause()
                .isInstanceOf(ElasticsearchWriteException.class)
                .hasMessageContaining("document 'broken'");
    }
}
