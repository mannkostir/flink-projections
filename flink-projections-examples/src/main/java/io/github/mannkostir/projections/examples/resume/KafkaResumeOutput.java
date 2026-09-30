package io.github.mannkostir.projections.examples.resume;

import org.apache.flink.streaming.api.datastream.DataStream;

import io.github.mannkostir.projections.Change;
import io.github.mannkostir.projections.kafka.KafkaChanges;
import io.github.mannkostir.projections.kafka.KafkaSinkOptions;

public final class KafkaResumeOutput implements ResumeOutput {
    @Override
    public void write(DataStream<Change<CandidateDoc>> documents, ResumeSearchConfig config) {
        String topic = config.topics().documents();
        KafkaSinkOptions options = KafkaSinkOptions.builder()
                .bootstrapServers(config.bootstrapServers())
                .topic(topic)
                .build();
        KafkaChanges.to("candidate-docs", documents, options, config.format().documents(topic));
    }
}
