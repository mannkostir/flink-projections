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
    void rejectsSecondEnrich() {
        StreamExecutionEnvironment env = ChangesTest.environment();
        Lookup.WithDimension<String, String> withDimension = Lookup
                .of("company", NestTest.changes(env, new Upsert<>("e1", "dev@k1")), LookupTest::companyOf, Types.STRING)
                .from(NestTest.changes(env, new Upsert<>("k1", "acme")), Types.STRING);
        withDimension.enrich((entity, company) -> entity, Types.STRING);

        assertThatThrownBy(() -> withDimension.enrich((entity, company) -> entity, Types.STRING))
                .isInstanceOf(ProjectionConfigurationException.class)
                .hasMessageContaining("already enriched");
    }

    @Test
    void rejectsEnrichThroughASecondDimensionOfTheSameLookup() {
        StreamExecutionEnvironment env = ChangesTest.environment();
        Lookup<String> lookup = Lookup.of("company", NestTest.changes(env, new Upsert<>("e1", "dev@k1")), LookupTest::companyOf, Types.STRING);
        Lookup.WithDimension<String, String> first = lookup.from(NestTest.changes(env, new Upsert<>("k1", "acme")), Types.STRING);
        Lookup.WithDimension<String, String> second = lookup.from(NestTest.changes(env, new Upsert<>("k1", "globex")), Types.STRING);
        first.enrich((entity, company) -> entity, Types.STRING);

        assertThatThrownBy(() -> second.enrich((entity, company) -> entity, Types.STRING))
                .isInstanceOf(ProjectionConfigurationException.class)
                .hasMessageContaining("already enriched");
    }

    @Test
    void rejectsDimensionAfterEnrich() {
        StreamExecutionEnvironment env = ChangesTest.environment();
        Lookup<String> lookup = Lookup.of("company", NestTest.changes(env, new Upsert<>("e1", "dev@k1")), LookupTest::companyOf, Types.STRING);
        lookup.from(NestTest.changes(env, new Upsert<>("k1", "acme")), Types.STRING).enrich((entity, company) -> entity, Types.STRING);
        DataStream<Change<String>> dimensions = NestTest.changes(env, new Upsert<>("k1", "globex"));

        assertThatThrownBy(() -> lookup.from(dimensions, Types.STRING))
                .isInstanceOf(ProjectionConfigurationException.class)
                .hasMessageContaining("already enriched");
    }

    @Test
    void rejectsOptionsAfterEnrich() {
        StreamExecutionEnvironment env = ChangesTest.environment();
        Lookup<String> lookup = Lookup.of("company", NestTest.changes(env, new Upsert<>("e1", "dev@k1")), LookupTest::companyOf, Types.STRING);
        lookup.from(NestTest.changes(env, new Upsert<>("k1", "acme")), Types.STRING).enrich((entity, company) -> entity, Types.STRING);
        LookupOptions options = LookupOptions.builder().requireMatch(true).build();

        assertThatThrownBy(() -> lookup.withOptions(options))
                .isInstanceOf(ProjectionConfigurationException.class)
                .hasMessageContaining("already enriched");
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
