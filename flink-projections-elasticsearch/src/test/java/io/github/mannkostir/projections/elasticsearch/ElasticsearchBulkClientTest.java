package io.github.mannkostir.projections.elasticsearch;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpServer;

class ElasticsearchBulkClientTest {
    private static final List<PendingOperation> ONE_DELETE = List.of(new DeleteOperation("a"));

    private HttpServer server;

    private BulkClient clientOfServerAnswering(int status) throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            byte[] body = "{\"error\":\"unavailable\"}".getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.getResponseHeaders().add("X-Elastic-Product", "Elasticsearch");
            exchange.sendResponseHeaders(status, body.length);
            try (OutputStream response = exchange.getResponseBody()) {
                response.write(body);
            }
        });
        server.start();
        return ElasticsearchBulkClient.open(ElasticsearchSinkOptions.builder()
                .hosts("http://127.0.0.1:" + server.getAddress().getPort())
                .index("docs")
                .build());
    }

    @AfterEach
    void stopServer() {
        server.stop(0);
    }

    @Test
    void serverErrorKeepsItsStatusSoItIsPermanent() throws IOException {
        try (BulkClient client = clientOfServerAnswering(500)) {
            assertThat(client.send(ONE_DELETE)).isInstanceOfSatisfying(RequestFailure.class,
                    failure -> assertThat(failure.status()).hasValue(500));
        }
    }

    @Test
    void unavailableKeepsItsStatusSoItIsRetried() throws IOException {
        try (BulkClient client = clientOfServerAnswering(503)) {
            assertThat(client.send(ONE_DELETE)).isInstanceOfSatisfying(RequestFailure.class,
                    failure -> assertThat(failure.status()).hasValue(503));
        }
    }
}
