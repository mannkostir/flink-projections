package io.github.mannkostir.projections.examples.resume;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;

final class ResumeScenario {
    private static final int PARTITIONS = 2;

    private final String bootstrapServers;
    private final ResumeTopics topics;

    ResumeScenario(String bootstrapServers, ResumeTopics topics) {
        this.bootstrapServers = bootstrapServers;
        this.topics = topics;
    }

    void createTopics() throws ExecutionException, InterruptedException {
        try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers))) {
            List<NewTopic> newTopics = List.of(
                    topics.candidates(), topics.skills(), topics.companies(), topics.experiences(), topics.projects(), topics.documents())
                    .stream().map(name -> new NewTopic(name, PARTITIONS, (short) 1)).toList();
            admin.createTopics(newTopics).all().get();
        }
    }

    void produce(ScenarioWriters writers) {
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerProperties())) {
            send(producer, topics.projects(), writers.projects(), Project::id,
                    new Project("p1", "e1", "search", false),
                    new Project("p2", "e1", "index", false),
                    new Project("p2", "e1", "index", true));
            send(producer, topics.experiences(), writers.experiences(), Experience::id,
                    new Experience("e1", "c1", "dev", "k1", null, false),
                    new Experience("e2", "c1", "qa", "k2", null, false),
                    new Experience("e2", "c2", "qa", "k2", null, false));
            send(producer, topics.companies(), writers.companies(), Company::id,
                    new Company("k1", "Acme", false),
                    new Company("k2", "Globex", false),
                    new Company("k1", "Acme Corp", false));
            send(producer, topics.skills(), writers.skills(), Skill::id,
                    new Skill("s1", "c1", "java", false),
                    new Skill("s2", "c3", "go", false));
            send(producer, topics.candidates(), writers.candidates(), Candidate::id,
                    new Candidate("c1", "alice", false),
                    new Candidate("c2", "bob", false),
                    new Candidate("c3", "carol", false),
                    new Candidate("c3", "carol", true));
            producer.flush();
        }
    }

    @SafeVarargs
    private static <T> void send(
            KafkaProducer<byte[], byte[]> producer,
            String topic,
            SerializationSchema<T> writer,
            java.util.function.Function<T, String> id,
            T... values) {
        for (T value : values) {
            producer.send(new ProducerRecord<>(topic, id.apply(value).getBytes(StandardCharsets.UTF_8), writer.serialize(value)));
        }
    }

    private Properties producerProperties() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        return properties;
    }
}
