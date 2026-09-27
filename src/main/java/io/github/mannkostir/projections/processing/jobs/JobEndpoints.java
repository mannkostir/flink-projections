package io.github.mannkostir.projections.processing.jobs;

import org.apache.flink.connector.kafka.sink.KafkaSink;

public record JobEndpoints<Source, Out>(Source source, KafkaSink<Out> sink) {
}
