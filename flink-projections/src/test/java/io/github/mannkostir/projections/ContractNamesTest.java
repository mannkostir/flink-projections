package io.github.mannkostir.projections;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class ContractNamesTest {
    @Test
    void operatorUids() {
        assertThat(ContractNames.changesUid("rows")).isEqualTo("changes_rows");
        assertThat(ContractNames.nestUid("candidate")).isEqualTo("nest_candidate");
        assertThat(ContractNames.nestRouteUid("candidate", "experiences")).isEqualTo("nest_candidate_route_experiences");
        assertThat(ContractNames.lookupUid("company")).isEqualTo("lookup_company");
        assertThat(ContractNames.lookupRouteUid("company")).isEqualTo("lookup_company_route");
    }

    @Test
    void stateDescriptorNames() {
        assertThat(ContractNames.nestParentState("candidate")).isEqualTo("candidate.parent");
        assertThat(ContractNames.nestChildState("candidate", "experiences")).isEqualTo("candidate.child.experiences");
        assertThat(ContractNames.nestLastDocState("candidate")).isEqualTo("candidate.last-doc");
        assertThat(ContractNames.nestRouteState("candidate", "experiences")).isEqualTo("candidate.route.experiences.last");
        assertThat(ContractNames.lookupEntitiesState("company")).isEqualTo("company.entities");
        assertThat(ContractNames.lookupDimensionState("company")).isEqualTo("company.dimension");
        assertThat(ContractNames.lookupRouteState("company")).isEqualTo("company.route.last");
    }
}
