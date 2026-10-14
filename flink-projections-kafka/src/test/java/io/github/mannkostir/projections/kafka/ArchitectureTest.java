package io.github.mannkostir.projections.kafka;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static com.tngtech.archunit.library.freeze.FreezingArchRule.freeze;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

@AnalyzeClasses(packages = ArchitectureTest.KAFKA, importOptions = ImportOption.DoNotIncludeTests.class)
class ArchitectureTest {
    static final String KAFKA = "io.github.mannkostir.projections.kafka";

    @ArchTest
    static final ArchRule kafkaDoesNotDependOnOtherAdapters = freeze(noClasses().that().resideInAPackage(KAFKA + "..")
            .should().dependOnClassesThat().resideInAnyPackage(
                    "io.github.mannkostir.projections.elasticsearch..",
                    "io.github.mannkostir.projections.examples..",
                    "co.elastic..",
                    "org.elasticsearch.."));
}
