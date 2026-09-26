package io.github.mannkostir.flink.io.kafka.topics;

import io.github.mannkostir.flink.io.kafka.config.KafkaClientConfig;

public class InputKafkaTopic extends KafkaTopic {
    public InputKafkaTopic (String topicName, KafkaClientConfig config) {
        super(topicName + "_changelog", config);
    }
}
