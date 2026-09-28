package io.github.mannkostir.projections.examples.resume;

public record Experience(String id, String candidateId, String title, String companyId, String companyName, boolean deleted) {
    Experience withCompanyName(String name) {
        return new Experience(id, candidateId, title, companyId, name, deleted);
    }
}
