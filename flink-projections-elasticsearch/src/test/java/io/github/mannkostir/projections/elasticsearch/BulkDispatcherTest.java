package io.github.mannkostir.projections.elasticsearch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.Test;

class BulkDispatcherTest {
    private static final String INDEX = "docs";
    private static final Backoff BACKOFF = new Backoff(Duration.ofMillis(100), Duration.ofMillis(300));
    private static final PendingOperation A = new DeleteOperation("a");
    private static final PendingOperation B = new DeleteOperation("b");

    private final ScriptedBulkClient client = new ScriptedBulkClient();
    private final RecordingSleeper sleeper = new RecordingSleeper();

    private BulkDispatcher dispatcher(int maxRetries) {
        return new BulkDispatcher(client, INDEX, maxRetries, BACKOFF, sleeper);
    }

    private static ItemResults failed(String id, int status, String type) {
        return new ItemResults(List.of(new ItemFailure(id, status, type, "reason for " + id)));
    }

    @Test
    void sendsAllOperationsInOneBulk() throws Exception {
        dispatcher(3).dispatch(List.of(A, B));

        assertThat(client.sent()).containsExactly(List.of(A, B));
    }

    @Test
    void retriesOnlyTheTransientlyFailedItems() throws Exception {
        client.thenRespond(failed("b", 429, "es_rejected_execution_exception"));

        dispatcher(3).dispatch(List.of(A, B));

        assertThat(client.sent()).containsExactly(List.of(A, B), List.of(B));
    }

    @Test
    void waitsACappedExponentialBackoffBetweenRetries() throws Exception {
        client.thenRespond(failed("a", 429, "t")).thenRespond(failed("a", 503, "t")).thenRespond(failed("a", 502, "t"));

        dispatcher(5).dispatch(List.of(A));

        assertThat(sleeper.delays()).containsExactly(Duration.ofMillis(100), Duration.ofMillis(200), Duration.ofMillis(300));
    }

    @Test
    void failsWhenRetriesRunOut() {
        client.thenRespond(failed("a", 429, "t")).thenRespond(failed("a", 429, "t")).thenRespond(failed("a", 429, "t"));

        assertThatThrownBy(() -> dispatcher(2).dispatch(List.of(A)))
                .isInstanceOf(ElasticsearchWriteException.class)
                .hasMessageContaining("after 2 retries")
                .hasMessageContaining("document 'a' failed with status 429");
    }

    @Test
    void zeroRetriesFailsOnTheFirstTransientFailure() {
        client.thenRespond(failed("a", 429, "t"));

        assertThatThrownBy(() -> dispatcher(0).dispatch(List.of(A)))
                .isInstanceOf(ElasticsearchWriteException.class)
                .hasMessageContaining("after 0 retries");
    }

    @Test
    void permanentItemFailureFailsAtOnceNamingTheDocument() {
        client.thenRespond(failed("b", 400, "document_parsing_exception"));

        assertThatThrownBy(() -> dispatcher(3).dispatch(List.of(A, B)))
                .isInstanceOf(ElasticsearchWriteException.class)
                .hasMessageContaining("index 'docs'")
                .hasMessageContaining("document 'b' failed with status 400 document_parsing_exception: reason for b")
                .hasMessageContaining("check the index mapping");
    }

    @Test
    void permanentFailureAmongTransientOnesIsNotRetried() {
        client.thenRespond(new ItemResults(List.of(
                new ItemFailure("a", 429, "t", "busy"),
                new ItemFailure("b", 400, "mapper_parsing_exception", "bad"))));

        assertThatThrownBy(() -> dispatcher(3).dispatch(List.of(A, B))).isInstanceOf(ElasticsearchWriteException.class);
        assertThat(client.sent()).hasSize(1);
    }

    @Test
    void deleteAgainstAMissingIndexCountsAsDone() throws Exception {
        client.thenRespond(failed("a", 404, "index_not_found_exception"));

        dispatcher(3).dispatch(List.of(A));

        assertThat(client.sent()).hasSize(1);
    }

    @Test
    void upsertAgainstAMissingIndexFailsTheJob() {
        client.thenRespond(failed("c", 404, "index_not_found_exception"));

        assertThatThrownBy(() -> dispatcher(3).dispatch(List.of(new IndexOperation("c", new byte[] {'{', '}'}))))
                .isInstanceOf(ElasticsearchWriteException.class)
                .hasMessageContaining("create the index");
    }

    @Test
    void failureForADocumentThatWasNotSentFailsTheJob() {
        client.thenRespond(failed("z", 429, "t"));

        assertThatThrownBy(() -> dispatcher(3).dispatch(List.of(A)))
                .isInstanceOf(ElasticsearchWriteException.class)
                .hasMessageContaining("index 'docs'")
                .hasMessageContaining("document 'z'");
    }

    @Test
    void transientRequestFailureResendsTheWholeBulk() throws Exception {
        client.thenRespond(RequestFailure.unreachable("Connection refused"));

        dispatcher(3).dispatch(List.of(A, B));

        assertThat(client.sent()).containsExactly(List.of(A, B), List.of(A, B));
    }

    @Test
    void overloadedRequestIsRetried() throws Exception {
        client.thenRespond(RequestFailure.withStatus(503, "unavailable"));

        dispatcher(3).dispatch(List.of(A));

        assertThat(client.sent()).hasSize(2);
    }

    @Test
    void permanentRequestFailureFailsAtOnceWithAnAccessHint() {
        client.thenRespond(RequestFailure.withStatus(401, "unauthorized"));

        assertThatThrownBy(() -> dispatcher(3).dispatch(List.of(A)))
                .isInstanceOf(ElasticsearchWriteException.class)
                .hasMessageContaining("status 401")
                .hasMessageContaining("ElasticsearchSinkOptions.auth");
    }
}
