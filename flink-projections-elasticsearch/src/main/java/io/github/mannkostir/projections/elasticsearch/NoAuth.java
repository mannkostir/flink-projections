package io.github.mannkostir.projections.elasticsearch;

import java.util.Optional;

final class NoAuth extends ElasticsearchAuth {
    @Override
    Optional<String> authorizationHeader() {
        return Optional.empty();
    }

    @Override
    public String toString() {
        return "ElasticsearchAuth.none()";
    }
}
