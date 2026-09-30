package io.github.mannkostir.projections.examples.resume;

import java.util.Map;
import java.util.Optional;

interface DocumentView extends AutoCloseable {
    Map<String, Optional<CandidateDoc>> latestById();

    @Override
    void close();
}
