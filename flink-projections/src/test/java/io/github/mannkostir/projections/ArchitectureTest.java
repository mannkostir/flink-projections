package io.github.mannkostir.projections;

import static com.tngtech.archunit.base.DescribedPredicate.describe;
import static com.tngtech.archunit.core.domain.JavaClass.Predicates.INTERFACES;
import static com.tngtech.archunit.core.domain.JavaClass.Predicates.assignableTo;
import static com.tngtech.archunit.core.domain.JavaClass.Predicates.simpleNameEndingWith;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static com.tngtech.archunit.library.freeze.FreezingArchRule.freeze;

import com.tngtech.archunit.base.DescribedPredicate;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

@AnalyzeClasses(packages = ArchitectureTest.CORE, importOptions = ImportOption.DoNotIncludeTests.class)
class ArchitectureTest {
    static final String CORE = "io.github.mannkostir.projections";
    private static final String FLINK = "org.apache.flink";

    private static final DescribedPredicate<JavaClass> DOMAIN = simpleNameEndingWith("Rules")
            .or(INTERFACES.and(simpleNameEndingWith("State")))
            .or(assignableTo(Change.class))
            .as("are domain rules, state ports or the change model");

    private static final DescribedPredicate<JavaClass> FLINK_BINDING = describe(
            "wrap Flink in the core",
            javaClass -> javaClass.getPackageName().equals(CORE)
                    && javaClass.getDirectDependenciesFromSelf().stream()
                            .anyMatch(dependency -> dependency.getTargetClass().getPackageName().startsWith(FLINK)));

    @ArchTest
    static final ArchRule domainDoesNotDependOnFlink = freeze(noClasses().that(DOMAIN)
            .should().dependOnClassesThat().resideInAPackage(FLINK + ".."));

    @ArchTest
    static final ArchRule domainDoesNotDependOnFlinkBinding = freeze(noClasses().that(DOMAIN)
            .should().dependOnClassesThat(FLINK_BINDING));

    @ArchTest
    static final ArchRule coreDoesNotDependOnAdapters = freeze(noClasses().that().resideInAPackage(CORE)
            .should().dependOnClassesThat().resideInAnyPackage(
                    CORE + ".kafka..",
                    CORE + ".elasticsearch..",
                    CORE + ".examples..",
                    "org.apache.kafka..",
                    "org.apache.flink.connector.kafka..",
                    "co.elastic..",
                    "org.elasticsearch.."));
}
