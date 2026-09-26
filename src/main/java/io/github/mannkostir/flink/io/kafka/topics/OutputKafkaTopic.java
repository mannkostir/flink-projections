package io.github.mannkostir.flink.io.kafka.topics;

import io.github.mannkostir.flink.io.kafka.config.KafkaClientConfig;

public class OutputKafkaTopic extends KafkaTopic {
    public OutputKafkaTopic (String topicName, KafkaClientConfig config) {
        super(topicName + "_output", config);
    }
}
