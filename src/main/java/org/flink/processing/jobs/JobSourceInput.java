package org.flink.processing.jobs;

import org.apache.avro.specific.SpecificRecordBase;
import org.flink.io.kafka.topics.KafkaTopic;
import org.flink.processing.records.EnvelopeRecord;
import org.flink.processing.records.interfaces.IEnvelopeSpecificRecord;

public class JobSourceInput<SourcePayload extends SpecificRecordBase, SourceRecord extends EnvelopeRecord<SourcePayload>> {
    public final KafkaTopic inputTopic;
    public final String name;
    public final IEnvelopeSpecificRecord<SourcePayload, SourceRecord> transformer;

    public JobSourceInput (
            KafkaTopic inputTopic,
            String name,
            IEnvelopeSpecificRecord<SourcePayload, SourceRecord> transformer
    ) {
        this.inputTopic = inputTopic;
        this.name = name;
        this.transformer = transformer;
    }
}
