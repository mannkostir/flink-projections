package org.flink.io.kafka.topics;

import org.flink.io.kafka.config.KafkaClientConfig;

public class KafkaTopic {
    private final String prefixedTopicName;

    public KafkaTopic (String topicName, KafkaClientConfig kafkaConfig) {
        if (topicName.contains("-")) {
            throw new IllegalArgumentException("Topic name cannot contain '-'");
        }
        this.prefixedTopicName = kafkaConfig.namespace() + "_" + topicName;
    }

    public String getName () {
        return this.prefixedTopicName;
    }
}
