package io.github.mannkostir.projections.elasticsearch;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Set;

import io.github.mannkostir.projections.ProjectionConfigurationException;

final class Hosts {
    private static final String OPTION = "ElasticsearchSinkOptions.hosts";
    private static final Set<String> SCHEMES = Set.of("http", "https");

    private Hosts() {
    }

    static List<String> requireValid(List<String> hosts) {
        if (hosts.isEmpty()) {
            throw new ProjectionConfigurationException(OPTION + " is required: pass at least one URL, e.g. hosts(\"https://es.example.com:9200\")");
        }
        hosts.forEach(Hosts::requireValid);
        return List.copyOf(hosts);
    }

    private static void requireValid(String host) {
        URI uri = parse(host);
        boolean valid = uri.getScheme() != null && SCHEMES.contains(uri.getScheme())
                && uri.getHost() != null
                && uri.getUserInfo() == null
                && (uri.getRawPath() == null || uri.getRawPath().isEmpty())
                && uri.getRawQuery() == null
                && uri.getRawFragment() == null;
        if (!valid) {
            throw invalid(host);
        }
    }

    private static URI parse(String host) {
        if (host == null || host.isBlank()) {
            throw invalid(host);
        }
        try {
            return new URI(host);
        } catch (URISyntaxException e) {
            throw invalid(host);
        }
    }

    private static ProjectionConfigurationException invalid(String host) {
        return new ProjectionConfigurationException(
                OPTION + " entry '" + host + "' is invalid: use an http or https URL with a host and optional port, "
                        + "without path, query or credentials, e.g. \"https://es.example.com:9200\"; set credentials with auth(...)");
    }
}
