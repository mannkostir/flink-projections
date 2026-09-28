package io.github.mannkostir.projections.examples.resume;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.formats.json.JsonSerializationSchema;

public final class JsonResumeFormat implements ResumeFormat {
    @Override
    public DeserializationSchema<Candidate> candidates() {
        return new JsonDeserializationSchema<>(ResumeTypes.CANDIDATE);
    }

    @Override
    public DeserializationSchema<Skill> skills() {
        return new JsonDeserializationSchema<>(ResumeTypes.SKILL);
    }

    @Override
    public DeserializationSchema<Company> companies() {
        return new JsonDeserializationSchema<>(ResumeTypes.COMPANY);
    }

    @Override
    public DeserializationSchema<Experience> experiences() {
        return new JsonDeserializationSchema<>(ResumeTypes.EXPERIENCE);
    }

    @Override
    public DeserializationSchema<Project> projects() {
        return new JsonDeserializationSchema<>(ResumeTypes.PROJECT);
    }

    @Override
    public SerializationSchema<CandidateDoc> documents(String topic) {
        return new JsonSerializationSchema<>();
    }
}
