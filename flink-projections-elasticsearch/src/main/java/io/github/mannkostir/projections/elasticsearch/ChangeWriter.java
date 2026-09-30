package io.github.mannkostir.projections.elasticsearch;

import java.io.IOException;
import java.util.concurrent.ScheduledFuture;

import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.SinkWriter;

import io.github.mannkostir.projections.Change;
import io.github.mannkostir.projections.Delete;

final class ChangeWriter<T> implements SinkWriter<Change<T>> {
    private final SerializationSchema<T> format;
    private final BulkClient client;
    private final BulkDispatcher dispatcher;
    private final ElasticsearchSinkOptions options;
    private final ProcessingTimeService time;
    private final OperationBuffer buffer = new OperationBuffer();
    private ScheduledFuture<?> flushTimer;

    private ChangeWriter(SerializationSchema<T> format, BulkClient client, ElasticsearchSinkOptions options, ProcessingTimeService time, Sleeper sleeper) {
        this.format = format;
        this.client = client;
        this.options = options;
        this.time = time;
        this.dispatcher = new BulkDispatcher(client, options.index(), options.maxRetries(), options.retryBackoff(), sleeper);
    }

    static <T> ChangeWriter<T> start(SerializationSchema<T> format, BulkClient client, ElasticsearchSinkOptions options, ProcessingTimeService time, Sleeper sleeper) {
        ChangeWriter<T> writer = new ChangeWriter<>(format, client, options, time, sleeper);
        writer.scheduleFlush();
        return writer;
    }

    @Override
    public void write(Change<T> change, Context context) throws InterruptedException {
        buffer.add(toOperation(change));
        if (buffer.isFull(options.maxBatchActions(), options.maxBatchBytes())) {
            send();
        }
    }

    @Override
    public void flush(boolean endOfInput) throws InterruptedException {
        send();
    }

    @Override
    public void close() throws IOException {
        if (flushTimer != null) {
            flushTimer.cancel(false);
        }
        client.close();
    }

    private PendingOperation toOperation(Change<T> change) {
        return change instanceof Delete<T>
                ? new DeleteOperation(change.id())
                : new IndexOperation(change.id(), format.serialize(change.value()));
    }

    private void send() throws InterruptedException {
        if (!buffer.isEmpty()) {
            dispatcher.dispatch(buffer.drain());
        }
    }

    private void onFlushTimer(long timestamp) throws InterruptedException {
        send();
        scheduleFlush();
    }

    private void scheduleFlush() {
        flushTimer = time.registerTimer(time.getCurrentProcessingTime() + options.flushInterval().toMillis(), this::onFlushTimer);
    }
}
