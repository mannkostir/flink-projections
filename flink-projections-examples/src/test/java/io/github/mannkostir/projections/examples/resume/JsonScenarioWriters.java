package io.github.mannkostir.projections.examples.resume;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.json.JsonSerializationSchema;

final class JsonScenarioWriters implements ScenarioWriters {
    @Override
    public SerializationSchema<Candidate> candidates() {
        return opened(new JsonSerializationSchema<>());
    }

    @Override
    public SerializationSchema<Skill> skills() {
        return opened(new JsonSerializationSchema<>());
    }

    @Override
    public SerializationSchema<Company> companies() {
        return opened(new JsonSerializationSchema<>());
    }

    @Override
    public SerializationSchema<Experience> experiences() {
        return opened(new JsonSerializationSchema<>());
    }

    @Override
    public SerializationSchema<Project> projects() {
        return opened(new JsonSerializationSchema<>());
    }

    private static <T> SerializationSchema<T> opened(JsonSerializationSchema<T> schema) {
        schema.open(null);
        return schema;
    }
}
