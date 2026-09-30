package io.github.mannkostir.projections.elasticsearch;

import java.util.Map;

final class FailureHints {
    private static final String MAPPING = "check the index mapping against the documents your format produces";
    private static final String MISSING_INDEX = "create the index or write alias before starting the job";
    private static final String ACCESS = "check ElasticsearchSinkOptions.auth(...) and the user's write privileges on the index";
    private static final String GENERIC = "fix the cause above and restart the job from its last checkpoint";

    private static final Map<String, String> BY_ERROR_TYPE = Map.of(
            "mapper_parsing_exception", MAPPING,
            "document_parsing_exception", MAPPING,
            "strict_dynamic_mapping_exception", MAPPING,
            "index_not_found_exception", MISSING_INDEX);

    private static final Map<Integer, String> BY_STATUS = Map.of(
            401, ACCESS,
            403, ACCESS,
            404, MISSING_INDEX);

    private FailureHints() {
    }

    static String forItem(ItemFailure failure) {
        return BY_ERROR_TYPE.getOrDefault(failure.type(), BY_STATUS.getOrDefault(failure.status(), GENERIC));
    }

    static String forRequest(RequestFailure failure) {
        return failure.status().isPresent() ? BY_STATUS.getOrDefault(failure.status().getAsInt(), GENERIC) : GENERIC;
    }
}
