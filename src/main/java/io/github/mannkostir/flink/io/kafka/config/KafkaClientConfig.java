package io.github.mannkostir.flink.io.kafka.config;

import java.util.Properties;

public class KafkaClientConfig {
    public enum OffsetStrategy {
        EARLIEST,
        LATEST,
        COMMITED
    }

    private final String bootstrapServers;
    private final String namespace;
    private final String schemaRegistryUrl;
    private final OffsetStrategy offsetStrategy;
    private final Integer partitionsAmount;

    private KafkaClientConfig(Builder builder) {
        this.bootstrapServers = builder.bootstrapServers;
        this.namespace = builder.namespace;
        this.schemaRegistryUrl = builder.schemaRegistryUrl;
        this.offsetStrategy = builder.offsetStrategy;
        this.partitionsAmount = builder.partitionsAmount;
    }

    public String bootstrapServers() { return bootstrapServers; }
    public String namespace() { return namespace; }
    public String schemaRegistryUrl() { return schemaRegistryUrl; }
    public OffsetStrategy offsetStrategy() { return offsetStrategy; }
    public Integer partitionsAmount() { return partitionsAmount; }

    public Properties buildCommonProperties() {
        Properties p = new Properties();

        p.setProperty("bootstrap.servers", bootstrapServers);


        return p;
    }

    public Properties buildProducerProperties() {
        Properties p = new Properties();

        p.setProperty("max.request.size", "20971520");

        return p;
    }

    public Properties buildConsumerProperties() {
        Properties p = new Properties();

        return p;
    }

    public static final class Builder {
        private String bootstrapServers;
        private String namespace;
        private String schemaRegistryUrl;
        private OffsetStrategy offsetStrategy;
        private Integer partitionsAmount;

        public Builder bootstrapServers(String v) { this.bootstrapServers = v; return this; }
        public Builder namespace(String v) { this.namespace = v; return this; }
        public Builder schemaRegistryUrl(String v) { this.schemaRegistryUrl = v; return this; }
        public Builder offsetStrategy(OffsetStrategy v) { this.offsetStrategy = v; return this; }
        public Builder partitionsAmount(Integer v) { this.partitionsAmount = v; return this; }
    }
}
