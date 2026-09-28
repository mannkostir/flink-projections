package io.github.mannkostir.projections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class LookupTest {
    @RegisterExtension
    static final MiniClusterExtension CLUSTER = new MiniClusterExtension();

    @Test
    void enrichesEntitiesWithDimension() throws Exception {
        StreamExecutionEnvironment env = ChangesTest.environment();
        DataStream<Change<String>> enriched = Lookup.of("company", NestTest.changes(env, new Upsert<>("e1", "dev@k1")), LookupTest::companyOf, Types.STRING)
                .withOptions(LookupOptions.builder().requireMatch(true).build())
                .from(NestTest.changes(env, new Upsert<>("k1", "acme")), Types.STRING)
                .enrich((entity, company) -> entity + "=" + company, Types.STRING);

        assertThat(enriched.executeAndCollect(10)).containsExactly(new Upsert<>("e1", "dev@k1=acme"));
    }

    @Test
    void namesOperatorsAfterLookup() {
        StreamExecutionEnvironment env = ChangesTest.environment();
        DataStream<Change<String>> enriched = Lookup.of("company", NestTest.changes(env, new Upsert<>("e1", "dev@k1")), LookupTest::companyOf, Types.STRING)
                .from(NestTest.changes(env, new Upsert<>("k1", "acme")), Types.STRING)
                .enrich((entity, company) -> entity, Types.STRING);

        assertThat(enriched.getTransformation().getUid()).isEqualTo("lookup_company");
        assertThat(NestTest.uidsUpstreamOf(enriched.getTransformation())).contains("lookup_company_route");
    }

    @Test
    void rejectsInvalidName() {
        StreamExecutionEnvironment env = ChangesTest.environment();

        assertThatThrownBy(() -> Lookup.of("company_name", NestTest.changes(env, new Upsert<>("e1", "dev@k1")), LookupTest::companyOf, Types.STRING))
                .isInstanceOf(ProjectionConfigurationException.class)
                .hasMessageContaining("'company_name'");
    }

    static String companyOf(String entity) {
        return entity.split("@")[1];
    }
}
