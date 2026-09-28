package io.github.mannkostir.projections.examples.resume;

import org.apache.flink.streaming.api.datastream.DataStream;

import io.github.mannkostir.projections.Change;
import io.github.mannkostir.projections.ChildSlot;
import io.github.mannkostir.projections.Lookup;
import io.github.mannkostir.projections.Nest;

public final class ResumeProjection {
    private ResumeProjection() {
    }

    public static DataStream<Change<CandidateDoc>> assemble(ResumeInputs inputs) {
        return candidateDocuments(inputs, experienceDocuments(inputs, experiencesWithCompany(inputs)));
    }

    private static DataStream<Change<Experience>> experiencesWithCompany(ResumeInputs inputs) {
        return Lookup.of("company", inputs.experiences(), Experience::companyId, ResumeTypes.EXPERIENCE)
                .from(inputs.companies(), ResumeTypes.COMPANY)
                .enrich((experience, company) -> experience.withCompanyName(company == null ? null : company.name()), ResumeTypes.EXPERIENCE);
    }

    private static DataStream<Change<ExperienceDoc>> experienceDocuments(ResumeInputs inputs, DataStream<Change<Experience>> experiences) {
        Nest<Experience> level = Nest.parent("experience", experiences, ResumeTypes.EXPERIENCE);
        ChildSlot<Project> projects = level.child("projects", inputs.projects(), Project::experienceId, ResumeTypes.PROJECT);
        return level.assemble(
                (experience, children) -> new ExperienceDoc(
                        experience.id(),
                        experience.candidateId(),
                        experience.title(),
                        experience.companyName(),
                        children.get(projects).stream().map(Project::name).toList()),
                ResumeTypes.EXPERIENCE_DOC);
    }

    private static DataStream<Change<CandidateDoc>> candidateDocuments(ResumeInputs inputs, DataStream<Change<ExperienceDoc>> experienceDocs) {
        Nest<Candidate> level = Nest.parent("candidate", inputs.candidates(), ResumeTypes.CANDIDATE);
        ChildSlot<ExperienceDoc> experiences = level.child("experiences", experienceDocs, ExperienceDoc::candidateId, ResumeTypes.EXPERIENCE_DOC);
        ChildSlot<Skill> skills = level.child("skills", inputs.skills(), Skill::candidateId, ResumeTypes.SKILL);
        return level.assemble(
                (candidate, children) -> new CandidateDoc(
                        candidate.id(),
                        candidate.name(),
                        children.get(experiences),
                        children.get(skills).stream().map(Skill::name).toList()),
                ResumeTypes.CANDIDATE_DOC);
    }
}
