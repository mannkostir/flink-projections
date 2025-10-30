package org.flink.io.kafka.topics;

import org.flink.io.kafka.config.KafkaClientConfig;

public class OutputKafkaTopic extends KafkaTopic {
    public OutputKafkaTopic (String topicName, KafkaClientConfig config) {
        //TODO: implement with prefix/postfix strategies
        super(topicName + "_output", config);
    }
}
