package io.github.mannkostir.projections.examples.resume;

import java.util.List;

public record ExperienceDoc(String id, String candidateId, String title, String companyName, List<String> projects) {
}
