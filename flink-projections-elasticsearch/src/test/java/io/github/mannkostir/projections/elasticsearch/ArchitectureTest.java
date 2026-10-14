package io.github.mannkostir.projections.elasticsearch;

import static com.tngtech.archunit.base.DescribedPredicate.not;
import static com.tngtech.archunit.core.domain.JavaClass.Predicates.belongToAnyOf;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static com.tngtech.archunit.library.freeze.FreezingArchRule.freeze;

import com.tngtech.archunit.base.DescribedPredicate;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

@AnalyzeClasses(packages = ArchitectureTest.ELASTICSEARCH, importOptions = ImportOption.DoNotIncludeTests.class)
class ArchitectureTest {
    static final String ELASTICSEARCH = "io.github.mannkostir.projections.elasticsearch";

    private static final DescribedPredicate<JavaClass> FLINK_SINK =
            belongToAnyOf(ElasticsearchChanges.class, ChangeSink.class, ChangeWriter.class).as("belong to the Flink sink");

    private static final DescribedPredicate<JavaClass> BULK_CLIENT =
            belongToAnyOf(ElasticsearchBulkClient.class).as("belong to the Elasticsearch bulk client");

    @ArchTest
    static final ArchRule elasticsearchDoesNotDependOnOtherAdapters = freeze(noClasses().that().resideInAPackage(ELASTICSEARCH + "..")
            .should().dependOnClassesThat().resideInAnyPackage(
                    "io.github.mannkostir.projections.kafka..",
                    "io.github.mannkostir.projections.examples..",
                    "org.apache.kafka..",
                    "org.apache.flink.connector.kafka.."));

    @ArchTest
    static final ArchRule onlyTheSinkDependsOnFlink = freeze(noClasses().that().resideInAPackage(ELASTICSEARCH + "..")
            .and(not(FLINK_SINK))
            .should().dependOnClassesThat().resideInAPackage("org.apache.flink.."));

    @ArchTest
    static final ArchRule onlyTheBulkClientDependsOnTheElasticsearchClient = freeze(noClasses().that().resideInAPackage(ELASTICSEARCH + "..")
            .and(not(BULK_CLIENT))
            .should().dependOnClassesThat().resideInAnyPackage("co.elastic..", "org.elasticsearch..", "org.apache.http.."));
}
