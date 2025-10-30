package org.flink.processing.jobs;

import org.apache.avro.specific.SpecificRecord;
import org.flink.io.kafka.topics.KafkaTopic;

public class JobSourceOutput<Out extends SpecificRecord> {
    public final KafkaTopic outputTopic;
    public final Class<Out> sourceClass;

    public JobSourceOutput (KafkaTopic outputTopic, Class<Out> sourceClass) {
        this.outputTopic = outputTopic;
        this.sourceClass = sourceClass;
    }
}
