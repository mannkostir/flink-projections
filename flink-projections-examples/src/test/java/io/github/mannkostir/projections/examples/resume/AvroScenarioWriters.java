package io.github.mannkostir.projections.examples.resume;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroSerializationSchema;
import org.apache.flink.util.function.SerializableFunction;

final class AvroScenarioWriters implements ScenarioWriters {
    private final String schemaRegistryUrl;
    private final ResumeTopics topics;

    AvroScenarioWriters(String schemaRegistryUrl, ResumeTopics topics) {
        this.schemaRegistryUrl = schemaRegistryUrl;
        this.topics = topics;
    }

    @Override
    public SerializationSchema<Candidate> candidates() {
        return writer(topics.candidates(), AvroSchemas.CANDIDATE, ResumeAvroRecords::fromCandidate);
    }

    @Override
    public SerializationSchema<Skill> skills() {
        return writer(topics.skills(), AvroSchemas.SKILL, ResumeAvroRecords::fromSkill);
    }

    @Override
    public SerializationSchema<Company> companies() {
        return writer(topics.companies(), AvroSchemas.COMPANY, ResumeAvroRecords::fromCompany);
    }

    @Override
    public SerializationSchema<Experience> experiences() {
        return writer(topics.experiences(), AvroSchemas.EXPERIENCE, ResumeAvroRecords::fromExperience);
    }

    @Override
    public SerializationSchema<Project> projects() {
        return writer(topics.projects(), AvroSchemas.PROJECT, ResumeAvroRecords::fromProject);
    }

    private <T> SerializationSchema<T> writer(String topic, Schema schema, SerializableFunction<T, GenericRecord> toRecord) {
        return new AvroRecordWriter<>(ConfluentRegistryAvroSerializationSchema.forGeneric(topic + "-value", schema, schemaRegistryUrl), toRecord);
    }
}
