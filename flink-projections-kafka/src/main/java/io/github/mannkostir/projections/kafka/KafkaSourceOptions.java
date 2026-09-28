package io.github.mannkostir.projections.kafka;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

public final class KafkaSourceOptions {
    private static final String OPTIONS = "KafkaSourceOptions";
    private static final Map<String, String> RESERVED = Map.of(
            "bootstrap.servers", "bootstrapServers(String)",
            "group.id", "groupId(String)");

    private final String bootstrapServers;
    private final String topic;
    private final String groupId;
    private final OffsetsInitializer startingOffsets;
    private final Properties properties;

    private KafkaSourceOptions(Builder builder) {
        this.bootstrapServers = OptionChecks.requireText(builder.bootstrapServers, OPTIONS + ".bootstrapServers");
        this.topic = OptionChecks.requireText(builder.topic, OPTIONS + ".topic");
        this.groupId = OptionChecks.requireText(builder.groupId, OPTIONS + ".groupId");
        this.startingOffsets = OptionChecks.requirePresent(builder.startingOffsets, OPTIONS + ".startingOffsets");
        this.properties = OptionChecks.properties(builder.properties, RESERVED, OPTIONS);
    }

    public static Builder builder() {
        return new Builder();
    }

    String bootstrapServers() {
        return bootstrapServers;
    }

    String topic() {
        return topic;
    }

    String groupId() {
        return groupId;
    }

    OffsetsInitializer startingOffsets() {
        return startingOffsets;
    }

    Properties properties() {
        Properties copy = new Properties();
        copy.putAll(properties);
        return copy;
    }

    public static final class Builder {
        private String bootstrapServers;
        private String topic;
        private String groupId;
        private OffsetsInitializer startingOffsets = OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST);
        private final Map<String, String> properties = new LinkedHashMap<>();

        private Builder() {
        }

        public Builder bootstrapServers(String bootstrapServers) {
            this.bootstrapServers = bootstrapServers;
            return this;
        }

        public Builder topic(String topic) {
            this.topic = topic;
            return this;
        }

        public Builder groupId(String groupId) {
            this.groupId = groupId;
            return this;
        }

        public Builder startingOffsets(OffsetsInitializer startingOffsets) {
            this.startingOffsets = startingOffsets;
            return this;
        }

        public Builder property(String key, String value) {
            this.properties.put(key, value);
            return this;
        }

        public KafkaSourceOptions build() {
            return new KafkaSourceOptions(this);
        }
    }
}
