package io.github.mannkostir.projections.elasticsearch;

import java.util.Optional;

final class ApiKeyAuth extends ElasticsearchAuth {
    private final String encodedApiKey;

    ApiKeyAuth(String encodedApiKey) {
        this.encodedApiKey = encodedApiKey;
    }

    @Override
    Optional<String> authorizationHeader() {
        return Optional.of("ApiKey " + encodedApiKey);
    }

    @Override
    public String toString() {
        return "ElasticsearchAuth.apiKey(****)";
    }
}
