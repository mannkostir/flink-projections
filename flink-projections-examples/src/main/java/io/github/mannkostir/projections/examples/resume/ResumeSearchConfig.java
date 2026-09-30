package io.github.mannkostir.projections.examples.resume;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;

import org.apache.flink.util.ParameterTool;

public record ResumeSearchConfig(String bootstrapServers, ResumeFormat format, ResumeTopics topics, ResumeOutput output) {
    private static final Map<String, Function<ParameterTool, ResumeFormat>> FORMATS = Map.of(
            "json", parameters -> new JsonResumeFormat(),
            "avro", parameters -> new AvroResumeFormat(required(parameters, "schema-registry-url", "--format avro")));

    private static final Map<String, Function<ParameterTool, ResumeOutput>> OUTPUTS = Map.of(
            "kafka", parameters -> new KafkaResumeOutput(),
            "elasticsearch", parameters -> new ElasticsearchResumeOutput(
                    Arrays.asList(required(parameters, "elasticsearch-hosts", "--output elasticsearch").split(",")),
                    parameters.get("elasticsearch-index", ElasticsearchResumeOutput.DEFAULT_INDEX)));

    public static ResumeSearchConfig parse(String[] args) {
        ParameterTool parameters = ParameterTool.fromArgs(args);
        return new ResumeSearchConfig(
                required(parameters, "bootstrap-servers", "the job"),
                format(parameters),
                ResumeTopics.defaults(),
                output(parameters));
    }

    private static ResumeFormat format(ParameterTool parameters) {
        String name = parameters.get("format", "json");
        Function<ParameterTool, ResumeFormat> format = FORMATS.get(name);
        if (format == null) {
            throw new ResumeSearchConfigException("--format '" + name + "' is not supported: use --format json or --format avro");
        }
        return format.apply(parameters);
    }

    private static ResumeOutput output(ParameterTool parameters) {
        String name = parameters.get("output", "kafka");
        Function<ParameterTool, ResumeOutput> output = OUTPUTS.get(name);
        if (output == null) {
            throw new ResumeSearchConfigException("--output '" + name + "' is not supported: use --output kafka or --output elasticsearch");
        }
        return output.apply(parameters);
    }

    private static String required(ParameterTool parameters, String key, String requiredBy) {
        String value = parameters.get(key);
        if (value == null || value.isBlank()) {
            throw new ResumeSearchConfigException("--" + key + " is required by " + requiredBy + ": pass --" + key + " <value>");
        }
        return value;
    }
}
