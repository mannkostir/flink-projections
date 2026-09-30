package io.github.mannkostir.projections.elasticsearch;

import java.io.IOException;
import java.util.List;

interface BulkClient extends AutoCloseable {
    BulkOutcome send(List<PendingOperation> operations);

    @Override
    void close() throws IOException;
}
