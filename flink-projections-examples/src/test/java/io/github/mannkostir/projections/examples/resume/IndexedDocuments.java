package io.github.mannkostir.projections.examples.resume;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.http.HttpHost;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;

final class IndexedDocuments implements DocumentView {
    private final RestClient client;
    private final String index;
    private final List<String> ids;
    private final DeserializationSchema<CandidateDoc> reader;

    IndexedDocuments(String host, String index, List<String> ids, DeserializationSchema<CandidateDoc> reader) {
        this.client = RestClient.builder(HttpHost.create(host)).build();
        this.index = index;
        this.ids = List.copyOf(ids);
        this.reader = reader;
    }

    @Override
    public Map<String, Optional<CandidateDoc>> latestById() {
        return ids.stream().collect(Collectors.toMap(Function.identity(), this::source));
    }

    private Optional<CandidateDoc> source(String id) {
        try {
            byte[] body = EntityUtils.toByteArray(client.performRequest(new Request("GET", "/" + index + "/_source/" + id)).getEntity());
            return Optional.of(reader.deserialize(body));
        } catch (ResponseException e) {
            return absentWhenNotFound(e);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static Optional<CandidateDoc> absentWhenNotFound(ResponseException e) {
        if (e.getResponse().getStatusLine().getStatusCode() == 404) {
            return Optional.empty();
        }
        throw new UncheckedIOException(e);
    }

    @Override
    public void close() {
        try {
            client.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
