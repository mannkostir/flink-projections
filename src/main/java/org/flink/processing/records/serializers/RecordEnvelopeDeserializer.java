package org.flink.processing.records.serializers;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.flink.processing.records.EnvelopeRecord;
import org.flink.processing.records.interfaces.IEnvelopeSpecificRecord;

public class RecordEnvelopeDeserializer<SourcePayload extends SpecificRecordBase, SourceRecord extends EnvelopeRecord<SourcePayload>>
        implements KafkaRecordDeserializationSchema<SourceRecord> {
    private final IEnvelopeSpecificRecord<SourcePayload, SourceRecord> transformer;
    private final String schemaRegistryUrl;

    public RecordEnvelopeDeserializer (IEnvelopeSpecificRecord<SourcePayload, SourceRecord> transformer, String schemaRegistryUrl) {
        super();
        this.transformer = transformer;
        this.schemaRegistryUrl = schemaRegistryUrl;
    }

    @Override
    public void deserialize (ConsumerRecord<byte[], byte[]> record, Collector<SourceRecord> out) {
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
    public TypeInformation<SourceRecord> getProducedType () {
        return this.transformer.envelopeTypeInformation();
    }
}
