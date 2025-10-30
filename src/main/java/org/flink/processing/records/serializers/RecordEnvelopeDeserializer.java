package org.flink.processing.records.serializers;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.flink.processing.records.EnvelopeRecord;
import org.flink.processing.records.interfaces.IEnvelopeSpecificRecord;

public class RecordEnvelopeDeserializer<SourcePayload extends SpecificRecordBase>
        implements KafkaRecordDeserializationSchema<EnvelopeRecord<SourcePayload>> {
    private final IEnvelopeSpecificRecord<SourcePayload, EnvelopeRecord<SourcePayload>> transformer;
    private final String schemaRegistryUrl;

    public RecordEnvelopeDeserializer (IEnvelopeSpecificRecord<SourcePayload, EnvelopeRecord<SourcePayload>> transformer, String schemaRegistryUrl) {
        super();
        this.transformer = transformer;
        this.schemaRegistryUrl = schemaRegistryUrl;
    }

    @Override
    public void deserialize (ConsumerRecord<byte[], byte[]> record, Collector<EnvelopeRecord<SourcePayload>> out) {
        try {
            var r = ConfluentRegistryAvroDeserializationSchema.forSpecific(
                    this.transformer.specificRecordClass(),
                    this.schemaRegistryUrl
            ).deserialize(record.value());

            out.collect(this.transformer.toEnvelope(r, new String(record.key())));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public TypeInformation<EnvelopeRecord<SourcePayload>> getProducedType () {
        return this.transformer.envelopeTypeInformation();
    }
}
