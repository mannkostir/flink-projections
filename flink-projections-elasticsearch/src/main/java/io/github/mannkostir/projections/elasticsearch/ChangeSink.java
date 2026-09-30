package io.github.mannkostir.projections.elasticsearch;

import java.io.IOException;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;

import io.github.mannkostir.projections.Change;

final class ChangeSink<T> implements Sink<Change<T>> {
    private final ElasticsearchSinkOptions options;
    private final SerializationSchema<T> format;
    private final BulkClientFactory clients;

    ChangeSink(ElasticsearchSinkOptions options, SerializationSchema<T> format, BulkClientFactory clients) {
        this.options = options;
        this.format = format;
        this.clients = clients;
    }

    @Override
    public SinkWriter<Change<T>> createWriter(WriterInitContext context) throws IOException {
        openFormat(context);
        return ChangeWriter.start(format, clients.create(options), options, context.getProcessingTimeService(), Sleeper.THREAD);
    }

    private void openFormat(WriterInitContext context) throws IOException {
        try {
            format.open(context.asSerializationSchemaInitializationContext());
        } catch (Exception e) {
            throw new IOException("Could not open the document format for index '" + options.index() + "'", e);
        }
    }
}
