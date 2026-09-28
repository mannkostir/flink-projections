package io.github.mannkostir.projections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class NestTest {
    @RegisterExtension
    static final MiniClusterExtension CLUSTER = new MiniClusterExtension();

    @Test
    void assemblesDocumentFromTwoChildSlots() throws Exception {
        StreamExecutionEnvironment env = ChangesTest.environment();
        Nest<String> candidates = Nest.parent("candidate", changes(env, new Upsert<>("c1", "alice")), Types.STRING);
        ChildSlot<String> skills = candidates.child("skills", changes(env, new Upsert<>("s1", "java@c1")), NestTest::parentOf, Types.STRING);
        ChildSlot<String> jobs = candidates.child("jobs", changes(env, new Upsert<>("j1", "acme@c1")), NestTest::parentOf, Types.STRING);

        DataStream<Change<String>> documents = candidates.assemble(
                (parent, children) -> parent + children.get(skills) + children.get(jobs), Types.STRING);

        assertThat(documents.executeAndCollect(10)).last().isEqualTo(new Upsert<>("c1", "alice[java@c1][acme@c1]"));
    }

    @Test
    void namesOperatorsAfterLevelAndSlots() {
        StreamExecutionEnvironment env = ChangesTest.environment();
        Nest<String> candidates = Nest.parent("candidate", changes(env, new Upsert<>("c1", "alice")), Types.STRING);
        candidates.child("skills", changes(env, new Upsert<>("s1", "java@c1")), NestTest::parentOf, Types.STRING);

        DataStream<Change<String>> documents = candidates.assemble((parent, children) -> parent, Types.STRING);

        assertThat(documents.getTransformation().getUid()).isEqualTo("nest_candidate");
        assertThat(uidsUpstreamOf(documents.getTransformation())).contains("nest_candidate_route_skills");
    }

    @Test
    void rejectsInvalidLevelName() {
        StreamExecutionEnvironment env = ChangesTest.environment();

        assertThatThrownBy(() -> Nest.parent("Candidate", changes(env, new Upsert<>("c1", "alice")), Types.STRING))
                .isInstanceOf(ProjectionConfigurationException.class)
                .hasMessageContaining("'Candidate'");
    }

    @Test
    void rejectsDuplicateSlotName() {
        StreamExecutionEnvironment env = ChangesTest.environment();
        Nest<String> candidates = Nest.parent("candidate", changes(env, new Upsert<>("c1", "alice")), Types.STRING);
        candidates.child("skills", changes(env, new Upsert<>("s1", "java@c1")), NestTest::parentOf, Types.STRING);

        assertThatThrownBy(() -> candidates.child("skills", changes(env, new Upsert<>("s2", "go@c1")), NestTest::parentOf, Types.STRING))
                .isInstanceOf(ProjectionConfigurationException.class)
                .hasMessageContaining("'skills'");
    }

    @Test
    void rejectsAssembleWithoutChildSlots() {
        StreamExecutionEnvironment env = ChangesTest.environment();
        Nest<String> candidates = Nest.parent("candidate", changes(env, new Upsert<>("c1", "alice")), Types.STRING);

        assertThatThrownBy(() -> candidates.assemble((parent, children) -> parent, Types.STRING))
                .isInstanceOf(ProjectionConfigurationException.class)
                .hasMessageContaining("child(...)");
    }

    @Test
    void rejectsSecondAssemble() {
        StreamExecutionEnvironment env = ChangesTest.environment();
        Nest<String> candidates = Nest.parent("candidate", changes(env, new Upsert<>("c1", "alice")), Types.STRING);
        candidates.child("skills", changes(env, new Upsert<>("s1", "java@c1")), NestTest::parentOf, Types.STRING);
        candidates.assemble((parent, children) -> parent, Types.STRING);

        assertThatThrownBy(() -> candidates.assemble((parent, children) -> parent, Types.STRING))
                .isInstanceOf(ProjectionConfigurationException.class)
                .hasMessageContaining("already assembled");
    }

    static String parentOf(String child) {
        return child.split("@")[1];
    }

    @SafeVarargs
    static DataStream<Change<String>> changes(StreamExecutionEnvironment env, Change<String>... changes) {
        return env.fromData(List.of(changes), Changes.typeInfo(Types.STRING));
    }

    static List<String> uidsUpstreamOf(Transformation<?> transformation) {
        return transformation.getTransitivePredecessors().stream()
                .map(Transformation::getUid)
                .filter(uid -> uid != null)
                .toList();
    }
}
