package io.github.mannkostir.projections.examples.resume;

import org.apache.flink.api.common.serialization.SerializationSchema;

interface ScenarioWriters {
    SerializationSchema<Candidate> candidates();

    SerializationSchema<Skill> skills();

    SerializationSchema<Company> companies();

    SerializationSchema<Experience> experiences();

    SerializationSchema<Project> projects();
}
