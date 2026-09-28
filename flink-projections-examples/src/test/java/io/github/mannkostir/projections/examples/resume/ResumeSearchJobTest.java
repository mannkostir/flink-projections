package io.github.mannkostir.projections.examples.resume;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

class ResumeSearchJobTest {
    @Test
    void wiresStableOperatorUids() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ResumeSearchJob.wire(env, new ResumeSearchConfig("localhost:9092", new JsonResumeFormat(), ResumeTopics.defaults()));

        assertThat(uids(env.getTransformations())).contains(
                "kafka_source_candidates", "changes_candidates",
                "kafka_source_skills", "changes_skills",
                "kafka_source_companies", "changes_companies",
                "kafka_source_experiences", "changes_experiences",
                "kafka_source_projects", "changes_projects",
                "lookup_company", "lookup_company_route",
                "nest_experience", "nest_experience_route_projects",
                "nest_candidate", "nest_candidate_route_experiences", "nest_candidate_route_skills",
                "kafka_sink_candidate-docs");
    }

    private static List<String> uids(Collection<Transformation<?>> roots) {
        return roots.stream().flatMap(ResumeSearchJobTest::withInputs).map(Transformation::getUid).distinct().toList();
    }

    private static Stream<Transformation<?>> withInputs(Transformation<?> transformation) {
        return Stream.concat(Stream.of(transformation), transformation.getInputs().stream().flatMap(ResumeSearchJobTest::withInputs));
    }
}
