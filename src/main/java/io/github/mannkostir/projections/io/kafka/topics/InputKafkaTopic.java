package io.github.mannkostir.projections.io.kafka.topics;

import io.github.mannkostir.projections.io.kafka.config.KafkaClientConfig;

public class InputKafkaTopic extends KafkaTopic {
    public InputKafkaTopic (String topicName, KafkaClientConfig config) {
        super(topicName + "_changelog", config);
    }
}
