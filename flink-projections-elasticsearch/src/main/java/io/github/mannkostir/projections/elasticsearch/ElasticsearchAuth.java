package io.github.mannkostir.projections.elasticsearch;

import java.io.Serializable;
import java.util.Optional;

public abstract sealed class ElasticsearchAuth implements Serializable permits NoAuth, BasicAuth, ApiKeyAuth {
    ElasticsearchAuth() {
    }

    public static ElasticsearchAuth none() {
        return new NoAuth();
    }

    public static ElasticsearchAuth basic(String username, String password) {
        return new BasicAuth(
                OptionChecks.requireText(username, "ElasticsearchAuth.basic username"),
                OptionChecks.requirePresent(password, "ElasticsearchAuth.basic password"));
    }

    public static ElasticsearchAuth apiKey(String encodedApiKey) {
        return new ApiKeyAuth(OptionChecks.requireText(encodedApiKey, "ElasticsearchAuth.apiKey encodedApiKey"));
    }

    abstract Optional<String> authorizationHeader();
}
