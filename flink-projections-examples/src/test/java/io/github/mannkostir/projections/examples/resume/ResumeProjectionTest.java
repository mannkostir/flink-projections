package io.github.mannkostir.projections.examples.resume;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.github.mannkostir.projections.Change;
import io.github.mannkostir.projections.Changes;
import io.github.mannkostir.projections.Delete;
import io.github.mannkostir.projections.Upsert;

class ResumeProjectionTest {
    @RegisterExtension
    static final MiniClusterExtension CLUSTER = new MiniClusterExtension();

    @Test
    void convergesToOneDocumentPerCandidate() throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(PipelineOptions.GENERIC_TYPES, false);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        ResumeInputs inputs = new ResumeInputs(
                stream(env, ResumeTypes.CANDIDATE,
                        new Upsert<>("c1", new Candidate("c1", "alice", false)),
                        new Upsert<>("c2", new Candidate("c2", "bob", false)),
                        new Upsert<>("c3", new Candidate("c3", "carol", false)),
                        new Delete<>("c3", new Candidate("c3", "carol", true))),
                stream(env, ResumeTypes.SKILL,
                        new Upsert<>("s1", new Skill("s1", "c1", "java", false)),
                        new Upsert<>("s2", new Skill("s2", "c3", "go", false))),
                stream(env, ResumeTypes.COMPANY,
                        new Upsert<>("k1", new Company("k1", "Acme", false)),
                        new Upsert<>("k2", new Company("k2", "Globex", false)),
                        new Upsert<>("k1", new Company("k1", "Acme Corp", false))),
                stream(env, ResumeTypes.EXPERIENCE,
                        new Upsert<>("e1", new Experience("e1", "c1", "dev", "k1", null, false)),
                        new Upsert<>("e2", new Experience("e2", "c1", "qa", "k2", null, false)),
                        new Upsert<>("e2", new Experience("e2", "c2", "qa", "k2", null, false))),
                stream(env, ResumeTypes.PROJECT,
                        new Upsert<>("p1", new Project("p1", "e1", "search", false)),
                        new Upsert<>("p2", new Project("p2", "e1", "index", false)),
                        new Delete<>("p2", new Project("p2", "e1", "index", true))));

        Map<String, Change<CandidateDoc>> latest = latestById(ResumeProjection.assemble(inputs).executeAndCollect(1000));

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
