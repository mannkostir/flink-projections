package io.github.mannkostir.projections.examples.resume;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

class ResumeAvroRecordsTest {
    @Test
    void candidateRoundTrips() {
        Candidate candidate = new Candidate("c1", "alice", true);

        assertThat(ResumeAvroRecords.toCandidate(ResumeAvroRecords.fromCandidate(candidate))).isEqualTo(candidate);
    }

    @Test
    void skillRoundTrips() {
        Skill skill = new Skill("s1", "c1", "java", false);

        assertThat(ResumeAvroRecords.toSkill(ResumeAvroRecords.fromSkill(skill))).isEqualTo(skill);
    }

    @Test
    void companyRoundTrips() {
        Company company = new Company("k1", "Acme", false);

        assertThat(ResumeAvroRecords.toCompany(ResumeAvroRecords.fromCompany(company))).isEqualTo(company);
    }

    @Test
    void experienceWithoutCompanyNameRoundTrips() {
        Experience experience = new Experience("e1", "c1", "dev", "k1", null, false);

        assertThat(ResumeAvroRecords.toExperience(ResumeAvroRecords.fromExperience(experience))).isEqualTo(experience);
    }

    @Test
    void projectRoundTrips() {
        Project project = new Project("p1", "e1", "search", true);

        assertThat(ResumeAvroRecords.toProject(ResumeAvroRecords.fromProject(project))).isEqualTo(project);
    }

    @Test
    void candidateDocRoundTrips() {
        CandidateDoc doc = new CandidateDoc("c1", "alice",
                List.of(new ExperienceDoc("e1", "c1", "dev", null, List.of("search", "index"))), List.of("java"));

        assertThat(ResumeAvroRecords.toCandidateDoc(ResumeAvroRecords.fromCandidateDoc(doc))).isEqualTo(doc);
    }
}
