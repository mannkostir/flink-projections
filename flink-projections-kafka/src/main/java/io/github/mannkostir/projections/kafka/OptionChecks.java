package io.github.mannkostir.projections.kafka;

import java.util.Map;
import java.util.Properties;

import io.github.mannkostir.projections.ProjectionConfigurationException;

final class OptionChecks {
    private OptionChecks() {
    }

    static String requireText(String value, String option) {
        if (value == null || value.isBlank()) {
            throw new ProjectionConfigurationException(option + " is required: set it to a non-blank value on the builder");
        }
        return value;
    }

    static <T> T requirePresent(T value, String option) {
        if (value == null) {
            throw new ProjectionConfigurationException(option + " must not be null: omit the call to keep the default");
        }
        return value;
    }

    static Properties properties(Map<String, String> entries, Map<String, String> reservedToOption, String options) {
        Properties properties = new Properties();
        entries.forEach((key, value) -> {
            requireNotReserved(key, reservedToOption, options);
            properties.setProperty(requireText(key, options + ".property key"), requireText(value, options + ".property('" + key + "') value"));
        });
        return properties;
    }

    private static void requireNotReserved(String key, Map<String, String> reservedToOption, String options) {
        if (reservedToOption.containsKey(key)) {
            throw new ProjectionConfigurationException(
                    options + ".property('" + key + "') is not allowed: use " + options + "." + reservedToOption.get(key) + " instead");
        }
    }
}
