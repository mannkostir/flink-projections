package org.flink.io.kafka.topics;

import org.flink.io.kafka.config.KafkaClientConfig;

public class InputKafkaTopic extends KafkaTopic {
    public InputKafkaTopic (String topicName, KafkaClientConfig config) {
        //TODO: implement with prefix/postfix strategies
        super(topicName + "_changelog", config);
    }
}
