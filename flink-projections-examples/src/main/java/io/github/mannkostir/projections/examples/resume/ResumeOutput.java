package io.github.mannkostir.projections.examples.resume;

import org.apache.flink.streaming.api.datastream.DataStream;

import io.github.mannkostir.projections.Change;

public interface ResumeOutput {
    void write(DataStream<Change<CandidateDoc>> documents, ResumeSearchConfig config);
}
