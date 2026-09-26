package io.github.mannkostir.flink.processing.jobs;

import org.apache.flink.connector.kafka.sink.KafkaSink;

public record JobEndpoints<Source, Out>(Source source, KafkaSink<Out> sink) {
}
