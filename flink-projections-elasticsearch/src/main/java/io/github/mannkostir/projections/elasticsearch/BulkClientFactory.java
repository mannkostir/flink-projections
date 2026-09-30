package io.github.mannkostir.projections.elasticsearch;

import java.io.Serializable;

interface BulkClientFactory extends Serializable {
    BulkClient create(ElasticsearchSinkOptions options);
}
