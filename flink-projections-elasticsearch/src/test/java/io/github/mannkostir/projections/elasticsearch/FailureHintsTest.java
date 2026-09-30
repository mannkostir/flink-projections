package io.github.mannkostir.projections.elasticsearch;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class FailureHintsTest {
    @Test
    void mappingErrorsPointAtTheMapping() {
        assertThat(FailureHints.forItem(new ItemFailure("a", 400, "mapper_parsing_exception", "r"))).contains("index mapping");
    }

    @Test
    void missingIndexAsksToCreateIt() {
        assertThat(FailureHints.forItem(new ItemFailure("a", 404, "index_not_found_exception", "r"))).contains("create the index");
    }

    @Test
    void forbiddenPointsAtAuth() {
        assertThat(FailureHints.forItem(new ItemFailure("a", 403, "security_exception", "r"))).contains("auth(...)");
    }

    @Test
    void unknownFailuresGetTheGenericHint() {
        assertThat(FailureHints.forItem(new ItemFailure("a", 500, "something", "r"))).contains("restart the job");
    }

    @Test
    void unreachableRequestsGetTheGenericHint() {
        assertThat(FailureHints.forRequest(RequestFailure.unreachable("refused"))).contains("restart the job");
    }

    @Test
    void tooLargeBatchPointsAtTheByteLimit() {
        assertThat(FailureHints.forRequest(RequestFailure.withStatus(413, "too large"))).contains("maxBatchBytes");
    }
}
