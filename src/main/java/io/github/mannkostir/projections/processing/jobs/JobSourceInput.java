package io.github.mannkostir.projections.processing.jobs;

import org.apache.avro.specific.SpecificRecordBase;
import io.github.mannkostir.projections.io.kafka.topics.KafkaTopic;
import io.github.mannkostir.projections.processing.records.EnvelopeRecord;
import io.github.mannkostir.projections.processing.records.interfaces.IEnvelopeSpecificRecord;

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
