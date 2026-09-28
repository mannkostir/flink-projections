package io.github.mannkostir.projections.examples.resume;

import java.time.Duration;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.awaitility.Awaitility;

final class ResumeSearchRun {
    private static final Duration CONVERGENCE_TIMEOUT = Duration.ofSeconds(60);

    private ResumeSearchRun() {
    }

    static void assertConverges(ResumeSearchConfig config, ScenarioWriters writers, DeserializationSchema<CandidateDoc> reader) throws Exception {
        ResumeScenario scenario = new ResumeScenario(config.bootstrapServers(), config.topics());
        scenario.createTopics();
        scenario.produce(writers);
        JobClient job = start(config);
        try (OutputTopic output = new OutputTopic(config.bootstrapServers(), config.topics().documents(), reader)) {
            Awaitility.await().atMost(CONVERGENCE_TIMEOUT).untilAsserted(() -> ExpectedDocuments.assertConverged(output.latestByKey()));
        } finally {
            job.cancel().get();
        }
    }

    private static JobClient start(ResumeSearchConfig config) throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(PipelineOptions.GENERIC_TYPES, false);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.enableCheckpointing(1_000);
        ResumeSearchJob.wire(env, config);
        return env.executeAsync("resume-search-it");
    }
}
