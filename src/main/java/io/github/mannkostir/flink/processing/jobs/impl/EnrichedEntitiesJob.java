package io.github.mannkostir.flink.processing.jobs.impl;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import io.github.mannkostir.flink.io.kafka.config.KafkaClientConfig;
import io.github.mannkostir.flink.processing.jobs.JobEndpoints;
import io.github.mannkostir.flink.processing.jobs.DataStreamWithKey;
import io.github.mannkostir.flink.processing.jobs.JobSourceInput;
import io.github.mannkostir.flink.processing.jobs.JobSourceOutput;
import io.github.mannkostir.flink.processing.jobs.interfaces.ICreateTransformedEnrichedRecords;
import io.github.mannkostir.flink.processing.records.EnrichedRecord;
import io.github.mannkostir.flink.processing.records.EnvelopeRecord;
import io.github.mannkostir.flink.processing.records.SinkRecord;
import io.github.mannkostir.flink.processing.records.interfaces.DeletableRecord;
import io.github.mannkostir.flink.processing.records.interfaces.IdentifiableRecord;
import io.github.mannkostir.flink.processing.workers.EnrichStreamWorker;

public abstract class EnrichedEntitiesJob<SourcePayload extends SpecificRecordBase, Source extends EnvelopeRecord<SourcePayload> & IdentifiableRecord & DeletableRecord
                                                       , ConnectedPayload extends SpecificRecordBase, Connected extends EnvelopeRecord<ConnectedPayload> & DeletableRecord, Enriched extends EnrichedRecord, OutPayload extends SpecificRecordBase, Out extends SinkRecord<OutPayload>>
        extends TransformJob<Enriched, OutPayload, Out> {
    private final JobSourceInput<SourcePayload, Source> sourceInput;
    private final JobSourceInput<ConnectedPayload, Connected> connectedInput;
    private final JobSourceOutput<OutPayload> sourceOutput;
    private final KeySelector<Source, String> sourceKeySelector;
    private final KeySelector<Connected, String> connectedKeySelector;

    protected ICreateTransformedEnrichedRecords<Source, Connected, Enriched, Out> recordsFactory;

    protected EnrichedEntitiesJob (
            ICreateTransformedEnrichedRecords<Source, Connected, Enriched, Out> recordsFactory,
            JobSourceInput<SourcePayload, Source> sourceInput,
            JobSourceInput<ConnectedPayload, Connected> connectedInput,
            JobSourceOutput<OutPayload> sourceOutput,
            KeySelector<Source, String> sourceKeySelector,
            KeySelector<Connected, String> connectedKeySelector,
            String jobName,
            KafkaClientConfig config
    ) {
        super(recordsFactory, jobName, config);
        this.sourceInput = sourceInput;
        this.connectedInput = connectedInput;
        this.sourceOutput = sourceOutput;
        this.sourceKeySelector = sourceKeySelector;
        this.connectedKeySelector = connectedKeySelector;
        this.recordsFactory = recordsFactory;
    }

    public DataStream<Source> getSourceStream () {
        return this.getStreamFromKafkaSource(this.sourceInput);
    }

    public DataStream<Connected> getConnectedStream () {
        return this.getStreamFromKafkaSource(this.connectedInput);
    }

    protected JobEndpoints<DataStream<Enriched>, Out> open () {
        DataStream<Source> sourceDataStream = this.getSourceStream();

        DataStream<Connected> connectedDataStream = this.getConnectedStream();

        DataStream<Enriched> enrichedStream = new EnrichStreamWorker<>(
                new DataStreamWithKey<>(sourceDataStream, this.sourceKeySelector),
                new DataStreamWithKey<>(connectedDataStream, this.connectedKeySelector),
                this.recordsFactory
        ).run(this.jobName);

        KafkaSink<Out> sink = this.getKafkaSink(this.sourceOutput);

        return new JobEndpoints<>(enrichedStream, sink);
    }
}
