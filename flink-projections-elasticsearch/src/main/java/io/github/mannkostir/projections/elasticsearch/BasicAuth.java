package io.github.mannkostir.projections.elasticsearch;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Optional;

final class BasicAuth extends ElasticsearchAuth {
    private final String username;
    private final String password;

    BasicAuth(String username, String password) {
        this.username = username;
        this.password = password;
    }

    @Override
    Optional<String> authorizationHeader() {
        String credentials = username + ":" + password;
        return Optional.of("Basic " + Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8)));
    }

    @Override
    public String toString() {
        return "ElasticsearchAuth.basic(username=" + username + ", password=****)";
    }
}
