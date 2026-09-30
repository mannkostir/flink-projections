package io.github.mannkostir.projections.examples.resume;

import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;

@Testcontainers
class ResumeSearchJsonIT {
    @RegisterExtension
    static final MiniClusterExtension CLUSTER = new MiniClusterExtension();

    @Container
    static final ConfluentKafkaContainer KAFKA = new ConfluentKafkaContainer("confluentinc/cp-kafka:7.4.0");

    @Test
    void projectsKafkaTopicsIntoCandidateDocuments() throws Exception {
        ResumeSearchConfig config = new ResumeSearchConfig(KAFKA.getBootstrapServers(), new JsonResumeFormat(), ResumeTopics.defaults(), KafkaResumeOutput.of(KAFKA.getBootstrapServers(), ResumeTopics.defaults(), new JsonResumeFormat()));
        JsonDeserializationSchema<CandidateDoc> reader = new JsonDeserializationSchema<>(ResumeTypes.CANDIDATE_DOC);
        reader.open(null);

        ResumeSearchRun.assertConverges(config, new JsonScenarioWriters(), () -> new OutputTopic(config.bootstrapServers(), config.topics().documents(), reader));
    }
}
