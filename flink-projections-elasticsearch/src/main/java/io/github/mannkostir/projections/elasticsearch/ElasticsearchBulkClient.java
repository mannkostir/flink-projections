package io.github.mannkostir.projections.elasticsearch;

import java.io.IOException;
import java.util.List;

import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.Time;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.json.SimpleJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.TransportException;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import co.elastic.clients.util.BinaryData;
import co.elastic.clients.util.ContentType;

final class ElasticsearchBulkClient implements BulkClient {
    private static final Time SERVER_TIMEOUT = Time.of(t -> t.time("20s"));

    private final ElasticsearchTransport transport;
    private final ElasticsearchClient client;
    private final String index;

    private ElasticsearchBulkClient(ElasticsearchTransport transport, String index) {
        this.transport = transport;
        this.client = new ElasticsearchClient(transport);
        this.index = index;
    }

    static BulkClient open(ElasticsearchSinkOptions options) {
        RestClient restClient = RestClient.builder(hosts(options))
                .setDefaultHeaders(headers(options.auth()))
                .build();
        return new ElasticsearchBulkClient(new RestClientTransport(restClient, new SimpleJsonpMapper()), options.index());
    }

    @Override
    public BulkOutcome send(List<PendingOperation> operations) {
        BulkRequest request = BulkRequest.of(bulk -> bulk.index(index).timeout(SERVER_TIMEOUT).operations(operations.stream().map(ElasticsearchBulkClient::toBulkOperation).toList()));
        try {
            return itemResults(client.bulk(request));
        } catch (ElasticsearchException e) {
            return RequestFailure.withStatus(e.status(), e.getMessage());
        } catch (TransportException e) {
            return e.statusCode() > 0 ? RequestFailure.withStatus(e.statusCode(), e.getMessage()) : RequestFailure.unreachable(e.getMessage());
        } catch (IOException e) {
            return requestFailure(e);
        }
    }

    @Override
    public void close() throws IOException {
        transport.close();
    }

    private static RequestFailure requestFailure(IOException failure) {
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            if (cause instanceof ResponseException response) {
                return RequestFailure.withStatus(response.getResponse().getStatusLine().getStatusCode(), response.getMessage());
            }
        }
        return RequestFailure.unreachable(failure.toString());
    }

    private static BulkOperation toBulkOperation(PendingOperation operation) {
        if (operation instanceof IndexOperation indexed) {
            BinaryData document = BinaryData.of(indexed.document(), ContentType.APPLICATION_JSON);
            return BulkOperation.of(bulk -> bulk.index(op -> op.id(indexed.id()).document(document)));
        }
        return BulkOperation.of(bulk -> bulk.delete(op -> op.id(operation.id())));
    }

    private static ItemResults itemResults(BulkResponse response) {
        if (!response.errors()) {
            return ItemResults.allSucceeded();
        }
        return new ItemResults(response.items().stream()
                .filter(item -> item.error() != null)
                .map(ElasticsearchBulkClient::toFailure)
                .toList());
    }

    private static ItemFailure toFailure(BulkResponseItem item) {
        return new ItemFailure(item.id(), item.status(), item.error().type(), item.error().reason());
    }

    private static HttpHost[] hosts(ElasticsearchSinkOptions options) {
        return options.hosts().stream().map(HttpHost::create).toArray(HttpHost[]::new);
    }

    private static Header[] headers(ElasticsearchAuth auth) {
        return auth.authorizationHeader()
                .map(value -> new Header[] {new BasicHeader("Authorization", value)})
                .orElseGet(() -> new Header[0]);
    }
}
