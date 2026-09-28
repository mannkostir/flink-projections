package io.github.mannkostir.projections.examples.resume;

public record ResumeTopics(String candidates, String skills, String companies, String experiences, String projects, String documents) {
    public static ResumeTopics defaults() {
        return new ResumeTopics(
                "resume.candidates",
                "resume.skills",
                "resume.companies",
                "resume.experiences",
                "resume.projects",
                "resume.candidate-docs");
    }
}
