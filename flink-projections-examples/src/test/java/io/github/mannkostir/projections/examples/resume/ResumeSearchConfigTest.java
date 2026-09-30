package io.github.mannkostir.projections.examples.resume;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.junit.jupiter.api.Test;

class ResumeSearchConfigTest {
    @Test
    void defaultsToJsonAndDefaultTopics() {
        ResumeSearchConfig config = ResumeSearchConfig.parse(new String[] {"--bootstrap-servers", "kafka:9092"});

        assertThat(config.bootstrapServers()).isEqualTo("kafka:9092");
        assertThat(config.format()).isInstanceOf(JsonResumeFormat.class);
        assertThat(config.topics()).isEqualTo(ResumeTopics.defaults());
    }

    @Test
    void defaultsToKafkaOutput() {
        ResumeSearchConfig config = ResumeSearchConfig.parse(new String[] {"--bootstrap-servers", "kafka:9092"});

        assertThat(config.output()).isInstanceOf(KafkaResumeOutput.class);
    }

    @Test
    void selectsElasticsearchOutputWithEveryHostAndTheDefaultIndex() {
        ResumeSearchConfig config = ResumeSearchConfig.parse(new String[] {
                "--bootstrap-servers", "kafka:9092", "--output", "elasticsearch", "--elasticsearch-hosts", "http://es1:9200,http://es2:9200"});

        assertThat(config.output()).isEqualTo(new ElasticsearchResumeOutput(List.of("http://es1:9200", "http://es2:9200"), "candidate-docs"));
    }

    @Test
    void takesTheElasticsearchIndex() {
        ResumeSearchConfig config = ResumeSearchConfig.parse(new String[] {
                "--bootstrap-servers", "kafka:9092", "--output", "elasticsearch", "--elasticsearch-hosts", "http://es:9200", "--elasticsearch-index", "resumes"});

        assertThat(config.output()).isEqualTo(new ElasticsearchResumeOutput(List.of("http://es:9200"), "resumes"));
    }

    @Test
    void elasticsearchOutputRequiresHosts() {
        assertThatThrownBy(() -> ResumeSearchConfig.parse(new String[] {"--bootstrap-servers", "kafka:9092", "--output", "elasticsearch"}))
                .isInstanceOf(ResumeSearchConfigException.class)
                .hasMessageContaining("--elasticsearch-hosts is required by --output elasticsearch");
    }

    @Test
    void rejectsUnknownOutput() {
        assertThatThrownBy(() -> ResumeSearchConfig.parse(new String[] {"--bootstrap-servers", "kafka:9092", "--output", "solr"}))
                .isInstanceOf(ResumeSearchConfigException.class)
                .hasMessageContaining("--output kafka or --output elasticsearch");
    }

    @Test
    void defaultTopicNames() {
        assertThat(ResumeTopics.defaults()).isEqualTo(new ResumeTopics(
                "resume.candidates", "resume.skills", "resume.companies", "resume.experiences", "resume.projects", "resume.candidate-docs"));
    }

    @Test
    void selectsAvroWithRegistry() {
        ResumeSearchConfig config = ResumeSearchConfig.parse(new String[] {
                "--bootstrap-servers", "kafka:9092", "--format", "avro", "--schema-registry-url", "http://registry:8081"});

        assertThat(config.format()).isInstanceOf(AvroResumeFormat.class);
    }

    @Test
    void requiresBootstrapServers() {
        assertThatThrownBy(() -> ResumeSearchConfig.parse(new String[] {}))
                .isInstanceOf(ResumeSearchConfigException.class)
                .hasMessageContaining("--bootstrap-servers");
    }

    @Test
    void avroRequiresRegistryUrl() {
        assertThatThrownBy(() -> ResumeSearchConfig.parse(new String[] {"--bootstrap-servers", "kafka:9092", "--format", "avro"}))
                .isInstanceOf(ResumeSearchConfigException.class)
                .hasMessageContaining("--schema-registry-url");
    }

    @Test
    void rejectsUnknownFormat() {
        assertThatThrownBy(() -> ResumeSearchConfig.parse(new String[] {"--bootstrap-servers", "kafka:9092", "--format", "xml"}))
                .isInstanceOf(ResumeSearchConfigException.class)
                .hasMessageContaining("--format json or --format avro");
    }
}
