package io.github.mannkostir.projections.kafka;

final class KafkaContractNames {
    private KafkaContractNames() {
    }

    static String sourceUid(String name) {
        return "kafka_source_" + name;
    }

    static String sinkUid(String name) {
        return "kafka_sink_" + name;
    }
}
