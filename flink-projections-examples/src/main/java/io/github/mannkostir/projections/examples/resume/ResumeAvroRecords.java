package io.github.mannkostir.projections.examples.resume;

import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

final class ResumeAvroRecords {
    private ResumeAvroRecords() {
    }

    static Candidate toCandidate(GenericRecord record) {
        return new Candidate(text(record, "id"), text(record, "name"), flag(record, "deleted"));
    }

    static GenericRecord fromCandidate(Candidate candidate) {
        return new GenericRecordBuilder(AvroSchemas.CANDIDATE)
                .set("id", candidate.id())
                .set("name", candidate.name())
                .set("deleted", candidate.deleted())
                .build();
    }

    static Skill toSkill(GenericRecord record) {
        return new Skill(text(record, "id"), text(record, "candidateId"), text(record, "name"), flag(record, "deleted"));
    }

    static GenericRecord fromSkill(Skill skill) {
        return new GenericRecordBuilder(AvroSchemas.SKILL)
                .set("id", skill.id())
                .set("candidateId", skill.candidateId())
                .set("name", skill.name())
                .set("deleted", skill.deleted())
                .build();
    }

    static Company toCompany(GenericRecord record) {
        return new Company(text(record, "id"), text(record, "name"), flag(record, "deleted"));
    }

    static GenericRecord fromCompany(Company company) {
        return new GenericRecordBuilder(AvroSchemas.COMPANY)
                .set("id", company.id())
                .set("name", company.name())
                .set("deleted", company.deleted())
                .build();
    }

    static Experience toExperience(GenericRecord record) {
        return new Experience(
                text(record, "id"),
                text(record, "candidateId"),
                text(record, "title"),
                text(record, "companyId"),
                text(record, "companyName"),
                flag(record, "deleted"));
    }

    static GenericRecord fromExperience(Experience experience) {
        return new GenericRecordBuilder(AvroSchemas.EXPERIENCE)
                .set("id", experience.id())
                .set("candidateId", experience.candidateId())
                .set("title", experience.title())
                .set("companyId", experience.companyId())
                .set("companyName", experience.companyName())
                .set("deleted", experience.deleted())
                .build();
    }

    static Project toProject(GenericRecord record) {
        return new Project(text(record, "id"), text(record, "experienceId"), text(record, "name"), flag(record, "deleted"));
    }

    static GenericRecord fromProject(Project project) {
        return new GenericRecordBuilder(AvroSchemas.PROJECT)
                .set("id", project.id())
                .set("experienceId", project.experienceId())
                .set("name", project.name())
                .set("deleted", project.deleted())
                .build();
    }

    static CandidateDoc toCandidateDoc(GenericRecord record) {
        return new CandidateDoc(
                text(record, "id"),
                text(record, "name"),
                records(record, "experiences").stream().map(ResumeAvroRecords::toExperienceDoc).toList(),
                texts(record, "skills"));
    }

    static GenericRecord fromCandidateDoc(CandidateDoc doc) {
        return new GenericRecordBuilder(AvroSchemas.CANDIDATE_DOC)
                .set("id", doc.id())
                .set("name", doc.name())
                .set("experiences", doc.experiences().stream().map(ResumeAvroRecords::fromExperienceDoc).toList())
                .set("skills", doc.skills())
                .build();
    }

    private static ExperienceDoc toExperienceDoc(GenericRecord record) {
        return new ExperienceDoc(
                text(record, "id"),
                text(record, "candidateId"),
                text(record, "title"),
                text(record, "companyName"),
                texts(record, "projects"));
    }

    private static GenericRecord fromExperienceDoc(ExperienceDoc doc) {
        return new GenericRecordBuilder(AvroSchemas.EXPERIENCE_DOC)
                .set("id", doc.id())
                .set("candidateId", doc.candidateId())
                .set("title", doc.title())
                .set("companyName", doc.companyName())
                .set("projects", doc.projects())
                .build();
    }

    private static String text(GenericRecord record, String field) {
        Object value = record.get(field);
        return value == null ? null : value.toString();
    }

    private static boolean flag(GenericRecord record, String field) {
        return (Boolean) record.get(field);
    }

    private static List<String> texts(GenericRecord record, String field) {
        return ((List<?>) record.get(field)).stream().map(Object::toString).toList();
    }

    @SuppressWarnings("unchecked")
    private static List<GenericRecord> records(GenericRecord record, String field) {
        return (List<GenericRecord>) record.get(field);
    }
}
