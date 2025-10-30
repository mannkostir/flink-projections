package org.flink.processing.jobs.impl;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.commons.math3.util.Pair;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.flink.io.kafka.config.KafkaClientConfig;
import org.flink.processing.jobs.DataStreamWithKey;
import org.flink.processing.jobs.JobSourceInput;
import org.flink.processing.jobs.JobSourceOutput;
import org.flink.processing.jobs.interfaces.ICreateTransformedEnrichedRecords;
import org.flink.processing.records.EnrichedRecord;
import org.flink.processing.records.EnvelopeRecord;
import org.flink.processing.records.SinkRecord;
import org.flink.processing.records.interfaces.DeletableRecord;
import org.flink.processing.records.interfaces.IdentifiableRecord;
import org.flink.processing.workers.EnrichStreamWorker;

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

    protected Pair<DataStream<Enriched>, KafkaSink<Out>> open () {
        DataStream<Source> sourceDataStream = this.getSourceStream();

        DataStream<Connected> connectedDataStream = this.getConnectedStream();

        DataStream<Enriched> enrichedStream = new EnrichStreamWorker<>(
                new DataStreamWithKey<>(sourceDataStream, this.sourceKeySelector),
                new DataStreamWithKey<>(connectedDataStream, this.connectedKeySelector),
                this.recordsFactory
        ).run(this.jobName);

        KafkaSink<Out> sink = this.getKafkaSink(this.sourceOutput);

        return new Pair<>(enrichedStream, sink);
    }
}
