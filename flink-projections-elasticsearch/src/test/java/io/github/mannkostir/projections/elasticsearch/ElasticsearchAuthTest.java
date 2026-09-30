package io.github.mannkostir.projections.elasticsearch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

import io.github.mannkostir.projections.ProjectionConfigurationException;

class ElasticsearchAuthTest {
    @Test
    void noneSendsNoAuthorizationHeader() {
        assertThat(ElasticsearchAuth.none().authorizationHeader()).isEmpty();
    }

    @Test
    void basicSendsBase64Credentials() {
        assertThat(ElasticsearchAuth.basic("elastic", "secret").authorizationHeader()).contains("Basic ZWxhc3RpYzpzZWNyZXQ=");
    }

    @Test
    void apiKeySendsTheEncodedKey() {
        assertThat(ElasticsearchAuth.apiKey("a2V5OnZhbHVl").authorizationHeader()).contains("ApiKey a2V5OnZhbHVl");
    }

    @Test
    void basicToStringHidesThePassword() {
        assertThat(ElasticsearchAuth.basic("elastic", "secret").toString()).contains("elastic").doesNotContain("secret");
    }

    @Test
    void apiKeyToStringHidesTheKey() {
        assertThat(ElasticsearchAuth.apiKey("a2V5OnZhbHVl").toString()).doesNotContain("a2V5OnZhbHVl");
    }

    @Test
    void basicRequiresAUsername() {
        assertThatThrownBy(() -> ElasticsearchAuth.basic(" ", "secret"))
                .isInstanceOf(ProjectionConfigurationException.class)
                .hasMessageContaining("ElasticsearchAuth.basic username");
    }

    @Test
    void basicRequiresAPassword() {
        assertThatThrownBy(() -> ElasticsearchAuth.basic("elastic", null))
                .isInstanceOf(ProjectionConfigurationException.class)
                .hasMessageContaining("ElasticsearchAuth.basic password");
    }

    @Test
    void apiKeyRequiresAKey() {
        assertThatThrownBy(() -> ElasticsearchAuth.apiKey(""))
                .isInstanceOf(ProjectionConfigurationException.class)
                .hasMessageContaining("ElasticsearchAuth.apiKey");
    }
}
