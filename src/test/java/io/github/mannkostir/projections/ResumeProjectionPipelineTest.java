package io.github.mannkostir.projections;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class ResumeProjectionPipelineTest {
    @RegisterExtension
    static final MiniClusterExtension CLUSTER = new MiniClusterExtension();

    public record Candidate(String id, String name) {
    }

    public record Skill(String id, String candidateId, String name) {
    }

    public record Company(String id, String name) {
    }

    public record Experience(String id, String candidateId, String title, String companyId, String companyName) {
        Experience withCompanyName(String name) {
            return new Experience(id, candidateId, title, companyId, name);
        }
    }

    public record Project(String id, String experienceId, String name) {
    }

    public record ExperienceDoc(String id, String candidateId, String title, String companyName, List<String> projects) {
    }

    public record CandidateDoc(String id, String name, List<ExperienceDoc> experiences, List<String> skills) {
    }

    private static final TypeInformation<Candidate> CANDIDATE = TypeInformation.of(Candidate.class);
    private static final TypeInformation<Skill> SKILL = TypeInformation.of(Skill.class);
    private static final TypeInformation<Company> COMPANY = TypeInformation.of(Company.class);
    private static final TypeInformation<Experience> EXPERIENCE = TypeInformation.of(Experience.class);
    private static final TypeInformation<Project> PROJECT = TypeInformation.of(Project.class);
    private static final TypeInformation<ExperienceDoc> EXPERIENCE_DOC = TypeInformation.of(ExperienceDoc.class);
    private static final TypeInformation<CandidateDoc> CANDIDATE_DOC = TypeInformation.of(CandidateDoc.class);

    @Test
    void convergesToOneDocumentPerCandidate() throws Exception {
        StreamExecutionEnvironment env = ChangesTest.environment();

        DataStream<Change<Candidate>> candidates = stream(env, CANDIDATE,
                new Upsert<>("c1", new Candidate("c1", "alice")),
                new Upsert<>("c2", new Candidate("c2", "bob")),
                new Upsert<>("c3", new Candidate("c3", "carol")),
                new Delete<>("c3", new Candidate("c3", "carol")));
        DataStream<Change<Skill>> skills = stream(env, SKILL,
                new Upsert<>("s1", new Skill("s1", "c1", "java")),
                new Upsert<>("s2", new Skill("s2", "c3", "go")));
        DataStream<Change<Company>> companies = stream(env, COMPANY,
                new Upsert<>("k1", new Company("k1", "Acme")),
                new Upsert<>("k2", new Company("k2", "Globex")),
                new Upsert<>("k1", new Company("k1", "Acme Corp")));
        DataStream<Change<Experience>> experiences = stream(env, EXPERIENCE,
                new Upsert<>("e1", new Experience("e1", "c1", "dev", "k1", null)),
                new Upsert<>("e2", new Experience("e2", "c1", "qa", "k2", null)),
                new Upsert<>("e2", new Experience("e2", "c2", "qa", "k2", null)));
        DataStream<Change<Project>> projects = stream(env, PROJECT,
                new Upsert<>("p1", new Project("p1", "e1", "search")),
                new Upsert<>("p2", new Project("p2", "e1", "index")),
                new Delete<>("p2", new Project("p2", "e1", "index")));

        DataStream<Change<Experience>> withCompany = Lookup.of("company", experiences, Experience::companyId, EXPERIENCE)
                .from(companies, COMPANY)
                .enrich((experience, company) -> experience.withCompanyName(company == null ? null : company.name()), EXPERIENCE);

        Nest<Experience> experienceLevel = Nest.parent("experience", withCompany, EXPERIENCE);
        ChildSlot<Project> projectSlot = experienceLevel.child("projects", projects, Project::experienceId, PROJECT);
        DataStream<Change<ExperienceDoc>> experienceDocs = experienceLevel.assemble(
                (experience, children) -> new ExperienceDoc(
                        experience.id(),
                        experience.candidateId(),
                        experience.title(),
                        experience.companyName(),
                        children.get(projectSlot).stream().map(Project::name).toList()),
                EXPERIENCE_DOC);

        Nest<Candidate> candidateLevel = Nest.parent("candidate", candidates, CANDIDATE);
        ChildSlot<ExperienceDoc> experienceSlot = candidateLevel.child("experiences", experienceDocs, ExperienceDoc::candidateId, EXPERIENCE_DOC);
        ChildSlot<Skill> skillSlot = candidateLevel.child("skills", skills, Skill::candidateId, SKILL);
        DataStream<Change<CandidateDoc>> documents = candidateLevel.assemble(
                (candidate, children) -> new CandidateDoc(
                        candidate.id(),
                        candidate.name(),
                        children.get(experienceSlot),
                        children.get(skillSlot).stream().map(Skill::name).toList()),
                CANDIDATE_DOC);

        Map<String, Change<CandidateDoc>> latest = latestById(documents.executeAndCollect(1000));

        assertThat(latest.get("c1")).isEqualTo(new Upsert<>("c1", new CandidateDoc("c1", "alice",
                List.of(new ExperienceDoc("e1", "c1", "dev", "Acme Corp", List.of("search"))),
                List.of("java"))));
        assertThat(latest.get("c2")).isEqualTo(new Upsert<>("c2", new CandidateDoc("c2", "bob",
                List.of(new ExperienceDoc("e2", "c2", "qa", "Globex", List.of())),
                List.of())));
        assertThat(latest.get("c3")).isInstanceOf(Delete.class);
    }

    @SafeVarargs
    private static <T> DataStream<Change<T>> stream(StreamExecutionEnvironment env, TypeInformation<T> type, Change<T>... changes) {
        return env.fromData(List.of(changes), Changes.typeInfo(type));
    }

    private static <T> Map<String, Change<T>> latestById(List<Change<T>> changes) {
        Map<String, Change<T>> latest = new LinkedHashMap<>();
        changes.forEach(change -> latest.put(change.id(), change));
        return latest;
    }
}
