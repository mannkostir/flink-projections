package io.github.mannkostir.projections.examples.resume;

import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;

@Testcontainers
class ResumeSearchAvroIT {
    private static final Network NETWORK = Network.newNetwork();
    private static final int REGISTRY_PORT = 8081;

    @RegisterExtension
    static final MiniClusterExtension CLUSTER = new MiniClusterExtension();

    @Container
    static final ConfluentKafkaContainer KAFKA = new ConfluentKafkaContainer("confluentinc/cp-kafka:7.4.0")
            .withNetwork(NETWORK)
            .withListener("kafka:19092");

    @Container
    static final GenericContainer<?> REGISTRY = new GenericContainer<>("confluentinc/cp-schema-registry:7.4.0")
            .withNetwork(NETWORK)
            .withExposedPorts(REGISTRY_PORT)
            .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
            .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:" + REGISTRY_PORT)
            .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://kafka:19092")
            .dependsOn(KAFKA)
            .waitingFor(Wait.forHttp("/subjects").forStatusCode(200));

    @Test
    void projectsKafkaTopicsIntoCandidateDocuments() throws Exception {
        String registryUrl = "http://" + REGISTRY.getHost() + ":" + REGISTRY.getMappedPort(REGISTRY_PORT);
        ResumeSearchConfig config = new ResumeSearchConfig(KAFKA.getBootstrapServers(), new AvroResumeFormat(registryUrl), ResumeTopics.defaults(), new KafkaResumeOutput());
        AvroRecordReader<CandidateDoc> reader = new AvroRecordReader<>(
                ConfluentRegistryAvroDeserializationSchema.forGeneric(AvroSchemas.CANDIDATE_DOC, registryUrl),
                ResumeAvroRecords::toCandidateDoc,
                ResumeTypes.CANDIDATE_DOC);

        ResumeSearchRun.assertConverges(config, new AvroScenarioWriters(registryUrl, config.topics()),
                () -> new OutputTopic(config.bootstrapServers(), config.topics().documents(), reader));
    }
}
