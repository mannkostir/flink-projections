package io.github.mannkostir.projections.examples.resume;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;

import io.github.mannkostir.projections.Change;
import io.github.mannkostir.projections.kafka.KafkaChanges;
import io.github.mannkostir.projections.kafka.KafkaSinkOptions;

public final class KafkaResumeOutput implements ResumeOutput {
    private final String bootstrapServers;
    private final String topic;
    private final SerializationSchema<CandidateDoc> format;

    private KafkaResumeOutput(String bootstrapServers, String topic, SerializationSchema<CandidateDoc> format) {
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        this.format = format;
    }

    public static KafkaResumeOutput of(String bootstrapServers, ResumeTopics topics, ResumeFormat format) {
        String topic = topics.documents();
        return new KafkaResumeOutput(bootstrapServers, topic, format.documents(topic));
    }

    @Override
    public void write(DataStream<Change<CandidateDoc>> documents) {
        KafkaSinkOptions options = KafkaSinkOptions.builder()
                .bootstrapServers(bootstrapServers)
                .topic(topic)
                .build();
        KafkaChanges.to("candidate-docs", documents, options, format);
    }
}
