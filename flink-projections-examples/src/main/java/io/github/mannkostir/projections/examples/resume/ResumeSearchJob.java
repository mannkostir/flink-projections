package io.github.mannkostir.projections.examples.resume;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import io.github.mannkostir.projections.Change;
import io.github.mannkostir.projections.kafka.DeleteDetection;
import io.github.mannkostir.projections.kafka.KafkaChanges;
import io.github.mannkostir.projections.kafka.KafkaSourceOptions;

public final class ResumeSearchJob {
    static final String GROUP_ID = "resume-search";
    private static final long CHECKPOINT_INTERVAL_MS = 10_000;

    private ResumeSearchJob() {
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(CHECKPOINT_INTERVAL_MS);
        wire(env, ResumeSearchConfig.parse(args));
        env.execute("resume-search");
    }

    public static void wire(StreamExecutionEnvironment env, ResumeSearchConfig config) {
        ResumeFormat format = config.format();
        ResumeTopics topics = config.topics();
        ResumeInputs inputs = new ResumeInputs(
                KafkaChanges.from("candidates", env, source(config, topics.candidates()), format.candidates(),
                        Candidate::id, DeleteDetection.flag(Candidate::deleted), ResumeTypes.CANDIDATE),
                KafkaChanges.from("skills", env, source(config, topics.skills()), format.skills(),
                        Skill::id, DeleteDetection.flag(Skill::deleted), ResumeTypes.SKILL),
                KafkaChanges.from("companies", env, source(config, topics.companies()), format.companies(),
                        Company::id, DeleteDetection.flag(Company::deleted), ResumeTypes.COMPANY),
                KafkaChanges.from("experiences", env, source(config, topics.experiences()), format.experiences(),
                        Experience::id, DeleteDetection.flag(Experience::deleted), ResumeTypes.EXPERIENCE),
                KafkaChanges.from("projects", env, source(config, topics.projects()), format.projects(),
                        Project::id, DeleteDetection.flag(Project::deleted), ResumeTypes.PROJECT));
        DataStream<Change<CandidateDoc>> documents = ResumeProjection.assemble(inputs);
        config.output().write(documents, config);
    }

    private static KafkaSourceOptions source(ResumeSearchConfig config, String topic) {
        return KafkaSourceOptions.builder()
                .bootstrapServers(config.bootstrapServers())
                .topic(topic)
                .groupId(GROUP_ID)
                .build();
    }
}
