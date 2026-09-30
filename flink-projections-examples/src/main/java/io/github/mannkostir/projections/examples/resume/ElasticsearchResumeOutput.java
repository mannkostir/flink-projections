package io.github.mannkostir.projections.examples.resume;

import java.util.List;

import org.apache.flink.formats.json.JsonSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;

import io.github.mannkostir.projections.Change;
import io.github.mannkostir.projections.elasticsearch.ElasticsearchChanges;
import io.github.mannkostir.projections.elasticsearch.ElasticsearchSinkOptions;

public record ElasticsearchResumeOutput(List<String> hosts, String index) implements ResumeOutput {
    public static final String DEFAULT_INDEX = "candidate-docs";

    public ElasticsearchResumeOutput {
        hosts = List.copyOf(hosts);
    }

    @Override
    public void write(DataStream<Change<CandidateDoc>> documents) {
        ElasticsearchSinkOptions options = ElasticsearchSinkOptions.builder()
                .hosts(hosts.toArray(String[]::new))
                .index(index)
                .build();
        ElasticsearchChanges.to("candidate-docs", documents, options, new JsonSerializationSchema<>());
    }
}
