package io.github.mannkostir.projections.examples.resume;

import org.apache.flink.streaming.api.datastream.DataStream;

import io.github.mannkostir.projections.Change;

public record ResumeInputs(
        DataStream<Change<Candidate>> candidates,
        DataStream<Change<Skill>> skills,
        DataStream<Change<Company>> companies,
        DataStream<Change<Experience>> experiences,
        DataStream<Change<Project>> projects) {
}
