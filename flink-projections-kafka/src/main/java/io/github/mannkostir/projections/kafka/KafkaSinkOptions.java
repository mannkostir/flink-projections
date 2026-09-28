package io.github.mannkostir.projections.kafka;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import org.apache.flink.connector.base.DeliveryGuarantee;

import io.github.mannkostir.projections.ProjectionConfigurationException;

public final class KafkaSinkOptions {
    private static final String OPTIONS = "KafkaSinkOptions";
    private static final Map<String, String> RESERVED = Map.of(
            "bootstrap.servers", "bootstrapServers(String)",
            "transactional.id", "transactionalIdPrefix(String)");

    private final String bootstrapServers;
    private final String topic;
    private final DeliveryGuarantee deliveryGuarantee;
    private final String transactionalIdPrefix;
    private final Properties properties;

    private KafkaSinkOptions(Builder builder) {
        this.bootstrapServers = OptionChecks.requireText(builder.bootstrapServers, OPTIONS + ".bootstrapServers");
        this.topic = OptionChecks.requireText(builder.topic, OPTIONS + ".topic");
        this.deliveryGuarantee = OptionChecks.requirePresent(builder.deliveryGuarantee, OPTIONS + ".deliveryGuarantee");
        this.transactionalIdPrefix = transactionalIdPrefixFor(deliveryGuarantee, builder.transactionalIdPrefix);
        this.properties = OptionChecks.properties(builder.properties, RESERVED, OPTIONS);
    }

    private static String transactionalIdPrefixFor(DeliveryGuarantee guarantee, String prefix) {
        boolean exactlyOnce = guarantee == DeliveryGuarantee.EXACTLY_ONCE;
        if (exactlyOnce) {
            return OptionChecks.requireText(prefix, OPTIONS + ".transactionalIdPrefix (required with EXACTLY_ONCE)");
        }
        if (prefix != null) {
            throw new ProjectionConfigurationException(
                    OPTIONS + ".transactionalIdPrefix is only used with EXACTLY_ONCE: remove it or set deliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)");
        }
        return null;
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

    DeliveryGuarantee deliveryGuarantee() {
        return deliveryGuarantee;
    }

    Optional<String> transactionalIdPrefix() {
        return Optional.ofNullable(transactionalIdPrefix);
    }

    Properties properties() {
        Properties copy = new Properties();
        copy.putAll(properties);
        return copy;
    }

    public static final class Builder {
        private String bootstrapServers;
        private String topic;
        private DeliveryGuarantee deliveryGuarantee = DeliveryGuarantee.AT_LEAST_ONCE;
        private String transactionalIdPrefix;
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

        public Builder deliveryGuarantee(DeliveryGuarantee deliveryGuarantee) {
            this.deliveryGuarantee = deliveryGuarantee;
            return this;
        }

        public Builder transactionalIdPrefix(String transactionalIdPrefix) {
            this.transactionalIdPrefix = transactionalIdPrefix;
            return this;
        }

        public Builder property(String key, String value) {
            this.properties.put(key, value);
            return this;
        }

        public KafkaSinkOptions build() {
            return new KafkaSinkOptions(this);
        }
    }
}
