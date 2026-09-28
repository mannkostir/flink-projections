package io.github.mannkostir.projections.examples.resume;

import java.util.Objects;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroSerializationSchema;
import org.apache.flink.util.function.SerializableFunction;

public final class AvroResumeFormat implements ResumeFormat {
    private final String schemaRegistryUrl;

    public AvroResumeFormat(String schemaRegistryUrl) {
        this.schemaRegistryUrl = Objects.requireNonNull(schemaRegistryUrl, "schemaRegistryUrl");
    }

    @Override
    public DeserializationSchema<Candidate> candidates() {
        return reader(AvroSchemas.CANDIDATE, ResumeAvroRecords::toCandidate, ResumeTypes.CANDIDATE);
    }

    @Override
    public DeserializationSchema<Skill> skills() {
        return reader(AvroSchemas.SKILL, ResumeAvroRecords::toSkill, ResumeTypes.SKILL);
    }

    @Override
    public DeserializationSchema<Company> companies() {
        return reader(AvroSchemas.COMPANY, ResumeAvroRecords::toCompany, ResumeTypes.COMPANY);
    }

    @Override
    public DeserializationSchema<Experience> experiences() {
        return reader(AvroSchemas.EXPERIENCE, ResumeAvroRecords::toExperience, ResumeTypes.EXPERIENCE);
    }

    @Override
    public DeserializationSchema<Project> projects() {
        return reader(AvroSchemas.PROJECT, ResumeAvroRecords::toProject, ResumeTypes.PROJECT);
    }

    @Override
    public SerializationSchema<CandidateDoc> documents(String topic) {
        return new AvroRecordWriter<>(
                ConfluentRegistryAvroSerializationSchema.forGeneric(topic + "-value", AvroSchemas.CANDIDATE_DOC, schemaRegistryUrl),
                ResumeAvroRecords::fromCandidateDoc);
    }

    private <T> DeserializationSchema<T> reader(Schema schema, SerializableFunction<GenericRecord, T> toValue, TypeInformation<T> type) {
        return new AvroRecordReader<>(ConfluentRegistryAvroDeserializationSchema.forGeneric(schema, schemaRegistryUrl), toValue, type);
    }
}
