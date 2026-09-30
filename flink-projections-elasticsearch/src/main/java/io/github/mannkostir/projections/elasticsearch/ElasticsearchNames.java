package io.github.mannkostir.projections.elasticsearch;

import java.util.regex.Pattern;

import io.github.mannkostir.projections.ProjectionConfigurationException;

final class ElasticsearchNames {
    private static final Pattern VALID = Pattern.compile("[a-z][a-z0-9-]*");

    private ElasticsearchNames() {
    }

    static String requireValid(String name, String role) {
        if (name == null || !VALID.matcher(name).matches()) {
            throw new ProjectionConfigurationException(
                    role + " '" + name + "' is invalid: use lowercase letters, digits and '-', starting with a letter, e.g. 'candidate-docs'");
        }
        return name;
    }
}
