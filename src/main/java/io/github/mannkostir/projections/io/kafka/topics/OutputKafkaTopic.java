package io.github.mannkostir.projections.io.kafka.topics;

import io.github.mannkostir.projections.io.kafka.config.KafkaClientConfig;

public class OutputKafkaTopic extends KafkaTopic {
    public OutputKafkaTopic (String topicName, KafkaClientConfig config) {
        super(topicName + "_output", config);
    }
}
