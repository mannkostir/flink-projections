package io.github.mannkostir.projections.io.kafka;

import java.util.*;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.ConfigResource;
import io.github.mannkostir.projections.io.kafka.config.KafkaClientConfig;
import io.github.mannkostir.projections.io.kafka.topics.KafkaTopic;
import io.github.mannkostir.projections.io.kafka.topics.OutputKafkaTopic;

public class KafkaAdmin {
    private static KafkaAdmin instance;
    private final AdminClient adminClient;
    private KafkaClientConfig config;

    private KafkaAdmin (KafkaClientConfig kafkaConfig) {
        this.config = kafkaConfig;

        Properties config = new Properties();
        config.put(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                kafkaConfig.bootstrapServers()
        );

        this.adminClient = AdminClient.create(config);
    }

    public static KafkaAdmin getInstance (KafkaClientConfig kafkaConfig) {
        if (instance == null) {
            instance = new KafkaAdmin(kafkaConfig);
        }

        return instance;
    }

    public void validateIfTopicExists (KafkaTopic topic) {
        try {
            ListTopicsResult listTopics = this.adminClient.listTopics();
            Set<String> names = listTopics.names().get();
            boolean contains = names.contains(topic.getName());
            if (!contains) {
                List<NewTopic> topicList = new ArrayList<>();
                Map<String, String> configs = new HashMap<>();
                if (topic instanceof OutputKafkaTopic) {
                    configs.put("cleanup.policy", "compact,delete");
                    configs.put("delete.retention.ms", "259200000");
                    configs.put("segment.ms", "600000");
                    configs.put("segment.bytes", "536870912");
                }
                int partitions = config.partitionsAmount();
                short replication = 1;
                NewTopic newTopic = new NewTopic(topic.getName(), partitions, replication).configs(
                        configs);
                topicList.add(newTopic);
                adminClient.createTopics(topicList);
            } else {
                if (topic instanceof OutputKafkaTopic) {
                    ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic.getName());
                    Collection<AlterConfigOp> ops = new ArrayList<>();
                    ops.add(new AlterConfigOp(new ConfigEntry("cleanup.policy", "compact,delete"), AlterConfigOp.OpType.SET));
                    ops.add(new AlterConfigOp(new ConfigEntry("delete.retention.ms", "259200000"), AlterConfigOp.OpType.SET));
                    ops.add(new AlterConfigOp(new ConfigEntry("segment.ms", "600000"), AlterConfigOp.OpType.SET));
                    ops.add(new AlterConfigOp(new ConfigEntry("segment.bytes", "536870912"), AlterConfigOp.OpType.SET));
                    Map<ConfigResource, Collection<AlterConfigOp>> alter = new HashMap<>();
                    alter.put(resource, ops);
                    adminClient.incrementalAlterConfigs(alter).all().get();
                }
            }
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
