package io.github.mannkostir.projections.examples.resume;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.Optional;

final class ExpectedDocuments {
    static final List<String> IDS = List.of("c1", "c2", "c3");

    private ExpectedDocuments() {
    }

    static void assertConverged(Map<String, Optional<CandidateDoc>> latest) {
        assertThat(latest).containsEntry("c1", Optional.of(new CandidateDoc("c1", "alice",
                List.of(new ExperienceDoc("e1", "c1", "dev", "Acme Corp", List.of("search"))),
                List.of("java"))));
        assertThat(latest).containsEntry("c2", Optional.of(new CandidateDoc("c2", "bob",
                List.of(new ExperienceDoc("e2", "c2", "qa", "Globex", List.of())),
                List.of())));
        assertThat(latest).containsEntry("c3", Optional.empty());
    }
}
