package io.github.mannkostir.projections.examples.resume;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.junit.jupiter.api.Test;

class JsonResumeFormatTest {
    private final JsonResumeFormat format = new JsonResumeFormat();

    @Test
    void readsCandidate() throws Exception {
        assertThat(read(format.candidates(), "{\"id\":\"c1\",\"name\":\"alice\",\"deleted\":true}"))
                .isEqualTo(new Candidate("c1", "alice", true));
    }

    @Test
    void readsSkill() throws Exception {
        assertThat(read(format.skills(), "{\"id\":\"s1\",\"candidateId\":\"c1\",\"name\":\"java\",\"deleted\":false}"))
                .isEqualTo(new Skill("s1", "c1", "java", false));
    }

    @Test
    void readsCompany() throws Exception {
        assertThat(read(format.companies(), "{\"id\":\"k1\",\"name\":\"Acme\",\"deleted\":false}"))
                .isEqualTo(new Company("k1", "Acme", false));
    }

    @Test
    void readsExperienceWithoutCompanyName() throws Exception {
        assertThat(read(format.experiences(), "{\"id\":\"e1\",\"candidateId\":\"c1\",\"title\":\"dev\",\"companyId\":\"k1\",\"deleted\":false}"))
                .isEqualTo(new Experience("e1", "c1", "dev", "k1", null, false));
    }

    @Test
    void readsProject() throws Exception {
        assertThat(read(format.projects(), "{\"id\":\"p1\",\"experienceId\":\"e1\",\"name\":\"search\",\"deleted\":false}"))
                .isEqualTo(new Project("p1", "e1", "search", false));
    }

    @Test
    void documentsRoundTrip() throws Exception {
        CandidateDoc doc = new CandidateDoc("c1", "alice",
                List.of(new ExperienceDoc("e1", "c1", "dev", "Acme", List.of("search"))), List.of("java"));
        SerializationSchema<CandidateDoc> writer = format.documents("resume.candidate-docs");
        writer.open(null);
        JsonDeserializationSchema<CandidateDoc> reader = new JsonDeserializationSchema<>(ResumeTypes.CANDIDATE_DOC);
        reader.open(null);

        assertThat(reader.deserialize(writer.serialize(doc))).isEqualTo(doc);
    }

    private static <T> T read(DeserializationSchema<T> schema, String json) throws Exception {
        schema.open(null);
        return schema.deserialize(json.getBytes(StandardCharsets.UTF_8));
    }
}
