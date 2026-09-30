package io.github.mannkostir.projections.elasticsearch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.github.mannkostir.projections.ProjectionConfigurationException;

class ElasticsearchSinkOptionsTest {
    private static ElasticsearchSinkOptions.Builder valid() {
        return ElasticsearchSinkOptions.builder().hosts("https://es.example.com:9200").index("docs");
    }

    @Test
    void defaults() {
        ElasticsearchSinkOptions options = valid().build();

        assertThat(options.auth()).isInstanceOf(NoAuth.class);
        assertThat(options.maxBatchActions()).isEqualTo(1000);
        assertThat(options.maxBatchBytes()).isEqualTo(5L * 1024 * 1024);
        assertThat(options.flushInterval()).isEqualTo(Duration.ofSeconds(1));
        assertThat(options.maxRetries()).isEqualTo(8);
        assertThat(options.retryBackoff()).isEqualTo(new Backoff(Duration.ofMillis(100), Duration.ofSeconds(10)));
    }

    @Test
    void keepsEveryHost() {
        assertThat(valid().hosts("http://a:9200", "https://b").build().hosts()).containsExactly("http://a:9200", "https://b");
    }

    @Test
    void requiresAHost() {
        assertThatThrownBy(() -> valid().hosts().build())
                .isInstanceOf(ProjectionConfigurationException.class)
                .hasMessageContaining("ElasticsearchSinkOptions.hosts is required");
    }

    @ParameterizedTest
    @ValueSource(strings = {"es:9200", "ftp://es:9200", "http://", "http://es:9200/prefix", "http://user:pw@es:9200", "http://es:9200?x=1", " "})
    void rejectsHostsThatAreNotPlainHttpUrls(String host) {
        assertThatThrownBy(() -> valid().hosts(host).build())
                .isInstanceOf(ProjectionConfigurationException.class)
                .hasMessageContaining("'" + host + "' is invalid");
    }

    @Test
    void requiresAnIndex() {
        assertThatThrownBy(() -> valid().index(null).build())
                .isInstanceOf(ProjectionConfigurationException.class)
                .hasMessageContaining("ElasticsearchSinkOptions.index is required");
    }

    @Test
    void rejectsNullAuth() {
        assertThatThrownBy(() -> valid().auth(null).build())
                .isInstanceOf(ProjectionConfigurationException.class)
                .hasMessageContaining("ElasticsearchSinkOptions.auth");
    }

    @Test
    void rejectsNonPositiveBatchActions() {
        assertThatThrownBy(() -> valid().maxBatchActions(0).build())
                .isInstanceOf(ProjectionConfigurationException.class)
                .hasMessageContaining("ElasticsearchSinkOptions.maxBatchActions must be greater than 0");
    }

    @Test
    void rejectsNonPositiveBatchBytes() {
        assertThatThrownBy(() -> valid().maxBatchBytes(-1).build())
                .isInstanceOf(ProjectionConfigurationException.class)
                .hasMessageContaining("ElasticsearchSinkOptions.maxBatchBytes must be greater than 0");
    }

    @Test
    void rejectsZeroFlushInterval() {
        assertThatThrownBy(() -> valid().flushInterval(Duration.ZERO).build())
                .isInstanceOf(ProjectionConfigurationException.class)
                .hasMessageContaining("ElasticsearchSinkOptions.flushInterval must be a positive duration");
    }

    @Test
    void allowsZeroRetries() {
        assertThat(valid().maxRetries(0).build().maxRetries()).isZero();
    }

    @Test
    void rejectsNegativeRetries() {
        assertThatThrownBy(() -> valid().maxRetries(-1).build())
                .isInstanceOf(ProjectionConfigurationException.class)
                .hasMessageContaining("ElasticsearchSinkOptions.maxRetries must be 0 or greater");
    }

    @Test
    void rejectsInitialBackoffAboveMax() {
        assertThatThrownBy(() -> valid().retryBackoff(Duration.ofSeconds(2), Duration.ofSeconds(1)).build())
                .isInstanceOf(ProjectionConfigurationException.class)
                .hasMessageContaining("pass initial <= max");
    }

    @Test
    void rejectsMissingBackoff() {
        assertThatThrownBy(() -> valid().retryBackoff(null, Duration.ofSeconds(1)).build())
                .isInstanceOf(ProjectionConfigurationException.class)
                .hasMessageContaining("ElasticsearchSinkOptions.retryBackoff initial");
    }
}
