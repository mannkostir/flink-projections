package io.github.mannkostir.flink.processing.records.interfaces;

import java.io.Serializable;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import io.github.mannkostir.flink.processing.records.EnvelopeRecord;

public interface IEnvelopeSpecificRecord<In extends SpecificRecordBase, Out extends EnvelopeRecord<In>>
        extends Serializable {
    Class<In> specificRecordClass ();

    Out toEnvelope (In data, String key);

    TypeInformation<Out> envelopeTypeInformation ();
}
