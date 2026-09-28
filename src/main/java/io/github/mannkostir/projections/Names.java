package io.github.mannkostir.projections;

import java.util.regex.Pattern;

final class Names {
    private static final Pattern VALID = Pattern.compile("[a-z][a-z0-9-]*");

    private Names() {
    }

    static String requireValid(String name, String role) {
        if (name == null || !VALID.matcher(name).matches()) {
            throw new ProjectionConfigurationException(
                    role + " '" + name + "' is invalid: use lowercase letters, digits and '-', starting with a letter, e.g. 'candidate'");
        }
        return name;
    }
}
