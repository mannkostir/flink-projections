package io.github.mannkostir.projections.elasticsearch;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

import org.apache.http.HttpHost;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;

final class IndexProbe implements AutoCloseable {
    private final RestClient client;

    IndexProbe(String httpHostAddress) {
        this.client = RestClient.builder(HttpHost.create(httpHostAddress)).build();
    }

    void createIndex(String index, String mappingJson) throws IOException {
        Request request = new Request("PUT", "/" + index);
        request.setJsonEntity(mappingJson);
        client.performRequest(request);
    }

    Optional<String> source(String index, String id) throws IOException {
        Request request = new Request("GET", "/" + index + "/_source/" + URLEncoder.encode(id, StandardCharsets.UTF_8).replace("+", "%20"));
        try {
            Response response = client.performRequest(request);
            return Optional.of(EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8));
        } catch (ResponseException e) {
            if (e.getResponse().getStatusLine().getStatusCode() == 404) {
                return Optional.empty();
            }
            throw e;
        }
    }

    @Override
    public void close() throws IOException {
        client.close();
    }
}
