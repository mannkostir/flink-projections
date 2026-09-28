package io.github.mannkostir.projections.kafka;

import java.util.regex.Pattern;

import io.github.mannkostir.projections.ProjectionConfigurationException;

final class KafkaNames {
    private static final Pattern VALID = Pattern.compile("[a-z][a-z0-9-]*");

    private KafkaNames() {
    }

    static String requireValid(String name, String role) {
        if (name == null || !VALID.matcher(name).matches()) {
            throw new ProjectionConfigurationException(
                    role + " '" + name + "' is invalid: use lowercase letters, digits and '-', starting with a letter, e.g. 'candidates'");
        }
        return name;
    }
}
