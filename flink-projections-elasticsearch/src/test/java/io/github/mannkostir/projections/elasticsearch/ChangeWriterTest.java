package io.github.mannkostir.projections.elasticsearch;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;
import org.junit.jupiter.api.Test;

import io.github.mannkostir.projections.Change;
import io.github.mannkostir.projections.Delete;
import io.github.mannkostir.projections.Upsert;

class ChangeWriterTest {
    private final ScriptedBulkClient client = new ScriptedBulkClient();
    private final TestProcessingTimeService time = new TestProcessingTimeService();

    private static ElasticsearchSinkOptions.Builder options() {
        return ElasticsearchSinkOptions.builder()
                .hosts("http://localhost:9200")
                .index("docs")
                .maxBatchActions(100)
                .flushInterval(Duration.ofSeconds(1));
    }

    private ChangeWriter<String> writer(ElasticsearchSinkOptions options) {
        return ChangeWriter.start(new SimpleStringSchema(), client, options, time, new RecordingSleeper());
    }

    private static IndexOperation index(String id, String json) {
        return new IndexOperation(id, json.getBytes(StandardCharsets.UTF_8));
    }

    private static void write(ChangeWriter<String> writer, Change<String> change) throws Exception {
        writer.write(change, null);
    }

    @Test
    void holdsChangesUntilAFlushTrigger() throws Exception {
        ChangeWriter<String> writer = writer(options().build());

        write(writer, new Upsert<>("a", "{}"));

        assertThat(client.sent()).isEmpty();
    }

    @Test
    void upsertIsIndexedWithTheFormatBytesAndDeleteByIdOnFlush() throws Exception {
        ChangeWriter<String> writer = writer(options().build());
        write(writer, new Upsert<>("a", "{\"name\":\"ü\"}"));
        write(writer, new Delete<>("b", "{}"));

        writer.flush(false);

        assertThat(client.sent()).containsExactly(List.of(index("a", "{\"name\":\"ü\"}"), new DeleteOperation("b")));
    }

    @Test
    void sendsWhenTheActionLimitIsReached() throws Exception {
        ChangeWriter<String> writer = writer(options().maxBatchActions(2).build());
        write(writer, new Upsert<>("a", "{}"));
        write(writer, new Upsert<>("b", "{}"));

        assertThat(client.sent()).hasSize(1);
    }

    @Test
    void coalescedChangesDoNotCountTowardsTheActionLimit() throws Exception {
        ChangeWriter<String> writer = writer(options().maxBatchActions(2).build());
        write(writer, new Upsert<>("a", "{}"));
        write(writer, new Delete<>("a", "{}"));

        assertThat(client.sent()).isEmpty();
    }

    @Test
    void aDocumentLargerThanTheByteLimitIsSentOnItsOwn() throws Exception {
        ChangeWriter<String> writer = writer(options().maxBatchBytes(10).build());

        write(writer, new Upsert<>("a", "{\"large\":true}"));

        assertThat(client.sent()).containsExactly(List.of(index("a", "{\"large\":true}")));
    }

    @Test
    void sendsWhenTheFlushIntervalPasses() throws Exception {
        ChangeWriter<String> writer = writer(options().build());
        write(writer, new Upsert<>("a", "{}"));

        time.advance(1_000);

        assertThat(client.sent()).hasSize(1);
    }

    @Test
    void keepsFlushingOnEveryInterval() throws Exception {
        ChangeWriter<String> writer = writer(options().build());
        write(writer, new Upsert<>("a", "{}"));
        time.advance(1_000);
        write(writer, new Upsert<>("b", "{}"));

        time.advance(1_000);

        assertThat(client.sent()).hasSize(2);
    }

    @Test
    void emptyBufferSendsNothing() throws Exception {
        ChangeWriter<String> writer = writer(options().build());

        writer.flush(true);
        time.advance(1_000);

        assertThat(client.sent()).isEmpty();
    }

    @Test
    void endOfInputDrainsTheBuffer() throws Exception {
        ChangeWriter<String> writer = writer(options().build());
        write(writer, new Upsert<>("a", "{}"));

        writer.flush(true);

        assertThat(client.sent()).hasSize(1);
    }

    @Test
    void closeClosesTheClientWithoutSending() throws Exception {
        ChangeWriter<String> writer = writer(options().build());
        write(writer, new Upsert<>("a", "{}"));

        writer.close();

        assertThat(client.sent()).isEmpty();
        assertThat(client.closed()).isTrue();
    }
}
