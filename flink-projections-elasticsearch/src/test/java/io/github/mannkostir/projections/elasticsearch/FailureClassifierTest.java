package io.github.mannkostir.projections.elasticsearch;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FailureClassifierTest {
    @ParameterizedTest
    @ValueSource(ints = {429, 502, 503, 504})
    void overloadAndGatewayStatusesAreTransient(int status) {
        assertThat(FailureClassifier.isTransient(status)).isTrue();
    }

    @ParameterizedTest
    @ValueSource(ints = {400, 401, 403, 404, 409, 500})
    void otherStatusesArePermanent(int status) {
        assertThat(FailureClassifier.isTransient(status)).isFalse();
    }
}
