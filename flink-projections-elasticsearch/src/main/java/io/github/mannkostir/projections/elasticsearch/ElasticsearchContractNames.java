package io.github.mannkostir.projections.elasticsearch;

final class ElasticsearchContractNames {
    private ElasticsearchContractNames() {
    }

    static String sinkUid(String name) {
        return "elasticsearch_sink_" + name;
    }
}
