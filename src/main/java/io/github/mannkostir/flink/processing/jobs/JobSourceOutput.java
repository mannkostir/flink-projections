package io.github.mannkostir.flink.processing.jobs;

import org.apache.avro.specific.SpecificRecord;
import io.github.mannkostir.flink.io.kafka.topics.KafkaTopic;

public class JobSourceOutput<Out extends SpecificRecord> {
    public final KafkaTopic outputTopic;
    public final Class<Out> sourceClass;

    public JobSourceOutput (KafkaTopic outputTopic, Class<Out> sourceClass) {
        this.outputTopic = outputTopic;
        this.sourceClass = sourceClass;
    }
}
