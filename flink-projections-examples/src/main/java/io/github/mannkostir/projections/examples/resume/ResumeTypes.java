package io.github.mannkostir.projections.examples.resume;

import org.apache.flink.api.common.typeinfo.TypeInformation;

final class ResumeTypes {
    static final TypeInformation<Candidate> CANDIDATE = TypeInformation.of(Candidate.class);
    static final TypeInformation<Skill> SKILL = TypeInformation.of(Skill.class);
    static final TypeInformation<Company> COMPANY = TypeInformation.of(Company.class);
    static final TypeInformation<Experience> EXPERIENCE = TypeInformation.of(Experience.class);
    static final TypeInformation<Project> PROJECT = TypeInformation.of(Project.class);
    static final TypeInformation<ExperienceDoc> EXPERIENCE_DOC = TypeInformation.of(ExperienceDoc.class);
    static final TypeInformation<CandidateDoc> CANDIDATE_DOC = TypeInformation.of(CandidateDoc.class);

    private ResumeTypes() {
    }
}
