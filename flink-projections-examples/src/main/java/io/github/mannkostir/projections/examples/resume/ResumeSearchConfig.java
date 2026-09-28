package io.github.mannkostir.projections.examples.resume;

import java.util.Map;
import java.util.function.Function;

import org.apache.flink.util.ParameterTool;

public record ResumeSearchConfig(String bootstrapServers, ResumeFormat format, ResumeTopics topics) {
    private static final Map<String, Function<ParameterTool, ResumeFormat>> FORMATS = Map.of(
            "json", parameters -> new JsonResumeFormat(),
            "avro", parameters -> new AvroResumeFormat(required(parameters, "schema-registry-url", "--format avro")));

    public static ResumeSearchConfig parse(String[] args) {
        ParameterTool parameters = ParameterTool.fromArgs(args);
        return new ResumeSearchConfig(
                required(parameters, "bootstrap-servers", "the job"),
                format(parameters),
                ResumeTopics.defaults());
    }

    private static ResumeFormat format(ParameterTool parameters) {
        String name = parameters.get("format", "json");
        Function<ParameterTool, ResumeFormat> format = FORMATS.get(name);
        if (format == null) {
            throw new ResumeSearchConfigException("--format '" + name + "' is not supported: use --format json or --format avro");
        }
        return format.apply(parameters);
    }

    private static String required(ParameterTool parameters, String key, String requiredBy) {
        String value = parameters.get(key);
        if (value == null || value.isBlank()) {
            throw new ResumeSearchConfigException("--" + key + " is required by " + requiredBy + ": pass --" + key + " <value>");
        }
        return value;
    }
}
