package org.flink.processing.jobs.impl;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.commons.math3.util.Pair;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.flink.io.kafka.config.KafkaClientConfig;
import org.flink.processing.functions.TransformMapFunction;
import org.flink.processing.jobs.base.Job;
import org.flink.processing.jobs.interfaces.ITransformRecords;
import org.flink.processing.records.SinkRecord;
import org.flink.processing.records.interfaces.CommonRecord;

public abstract class TransformJob<In extends CommonRecord, OutPayload extends SpecificRecordBase
                                          , Out extends SinkRecord<OutPayload>> extends Job {
    protected final ITransformRecords<In, Out> recordFactory;
    private final String name;

    protected TransformJob (ITransformRecords<In, Out> recordFactory, String jobName, KafkaClientConfig config) {
        super(jobName, config);
        this.recordFactory = recordFactory;
        this.name = this.recordFactory.transformedRecordTypeInformation()
                                      .getTypeClass()
                                      .getSimpleName();
    }

    protected abstract Pair<DataStream<In>, KafkaSink<Out>> open ();

    private void execute (DataStream<In> inputStream, KafkaSink<Out> outputSink) {
        DataStream<Out> transformedStream = inputStream.map(
                new TransformMapFunction<>(this.recordFactory),
                this.recordFactory.transformedRecordTypeInformation()
        ).name("transformed_to_" + this.name).uid("transformed_to_" + this.name)
                ;

        transformedStream.sinkTo(outputSink)
                         .name("transformed_sink_" + this.name)
                         .uid("transformed_sink_" + this.name)
        ;
    }

    @Override
    public final void run () {
        Pair<DataStream<In>, KafkaSink<Out>> pair = this.open();
        DataStream<In> kafkaSource = pair.getKey();
        KafkaSink<Out> sink = pair.getValue();

        this.execute(kafkaSource, sink);

    }
}
