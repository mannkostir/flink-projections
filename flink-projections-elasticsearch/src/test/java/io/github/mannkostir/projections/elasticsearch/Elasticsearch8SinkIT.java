package io.github.mannkostir.projections.elasticsearch;

import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class Elasticsearch8SinkIT extends ElasticsearchSinkContract {
    @Container
    static final ElasticsearchContainer ELASTICSEARCH = new ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch:8.19.22")
            .withEnv("xpack.security.enabled", "false")
            .withEnv("ES_JAVA_OPTS", "-Xms512m -Xmx512m");

    @Override
    ElasticsearchContainer elasticsearch() {
        return ELASTICSEARCH;
    }
}
