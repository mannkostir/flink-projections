package io.github.mannkostir.projections.elasticsearch;

record ItemFailure(String id, int status, String type, String reason) {
    private static final String MISSING_INDEX = "index_not_found_exception";

    boolean isMissingIndex() {
        return MISSING_INDEX.equals(type);
    }

    boolean isTransient() {
        return FailureClassifier.isTransient(status);
    }

    String describe() {
        return "document '" + id + "' failed with status " + status + " " + type + ": " + reason;
    }
}
