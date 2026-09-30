package io.github.mannkostir.projections.examples.resume;

import java.io.IOException;
import java.util.List;

import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.http.HttpHost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;

@Testcontainers
class ResumeSearchElasticsearchIT {
    @RegisterExtension
    static final MiniClusterExtension CLUSTER = new MiniClusterExtension();

    @Container
    static final ConfluentKafkaContainer KAFKA = new ConfluentKafkaContainer("confluentinc/cp-kafka:7.4.0");

    @Container
    static final ElasticsearchContainer ELASTICSEARCH = new ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch:9.5.3")
            .withEnv("xpack.security.enabled", "false")
            .withEnv("ES_JAVA_OPTS", "-Xms512m -Xmx512m");

    @Test
    void projectsKafkaTopicsIntoAnElasticsearchIndex() throws Exception {
        String host = "http://" + ELASTICSEARCH.getHttpHostAddress();
        ElasticsearchResumeOutput output = new ElasticsearchResumeOutput(List.of(host), ElasticsearchResumeOutput.DEFAULT_INDEX);
        ResumeSearchConfig config = new ResumeSearchConfig(KAFKA.getBootstrapServers(), new JsonResumeFormat(), ResumeTopics.defaults(), output);
        JsonDeserializationSchema<CandidateDoc> reader = new JsonDeserializationSchema<>(ResumeTypes.CANDIDATE_DOC);
        reader.open(null);
        createIndex(host, output.index());

        ResumeSearchRun.assertConverges(config, new JsonScenarioWriters(),
                () -> new IndexedDocuments(host, output.index(), ExpectedDocuments.IDS, reader));
    }

    private static void createIndex(String host, String index) throws IOException {
        try (RestClient client = RestClient.builder(HttpHost.create(host)).build()) {
            client.performRequest(new Request("PUT", "/" + index));
        }
    }
}
