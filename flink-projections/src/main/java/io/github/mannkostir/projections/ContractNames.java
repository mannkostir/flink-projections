package io.github.mannkostir.projections;

final class ContractNames {
    private ContractNames() {
    }

    static String changesUid(String name) {
        return "changes_" + name;
    }

    static String nestUid(String name) {
        return "nest_" + name;
    }

    static String nestRouteUid(String name, String slot) {
        return "nest_" + name + "_route_" + slot;
    }

    static String lookupUid(String name) {
        return "lookup_" + name;
    }

    static String lookupRouteUid(String name) {
        return "lookup_" + name + "_route";
    }

    static String nestParentState(String name) {
        return name + ".parent";
    }

    static String nestChildState(String name, String slot) {
        return name + ".child." + slot;
    }

    static String nestLastDocState(String name) {
        return name + ".last-doc";
    }

    static String nestRouteState(String name, String slot) {
        return name + ".route." + slot + ".last";
    }

    static String lookupEntitiesState(String name) {
        return name + ".entities";
    }

    static String lookupDimensionState(String name) {
        return name + ".dimension";
    }

    static String lookupRouteState(String name) {
        return name + ".route.last";
    }
}
