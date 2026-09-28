package io.github.mannkostir.projections.examples.resume;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;

public interface ResumeFormat {
    DeserializationSchema<Candidate> candidates();

    DeserializationSchema<Skill> skills();

    DeserializationSchema<Company> companies();

    DeserializationSchema<Experience> experiences();

    DeserializationSchema<Project> projects();

    SerializationSchema<CandidateDoc> documents(String topic);
}
