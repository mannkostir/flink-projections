package io.github.mannkostir.projections.examples.resume;

import java.util.List;

public record CandidateDoc(String id, String name, List<ExperienceDoc> experiences, List<String> skills) {
}
