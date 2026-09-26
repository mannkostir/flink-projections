package io.github.mannkostir.flink.processing.jobs.impl;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import io.github.mannkostir.flink.io.kafka.config.KafkaClientConfig;
import io.github.mannkostir.flink.processing.jobs.JobEndpoints;
import io.github.mannkostir.flink.processing.functions.TransformMapFunction;
import io.github.mannkostir.flink.processing.jobs.base.Job;
import io.github.mannkostir.flink.processing.jobs.interfaces.ITransformRecords;
import io.github.mannkostir.flink.processing.records.SinkRecord;
import io.github.mannkostir.flink.processing.records.interfaces.CommonRecord;

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

    protected abstract JobEndpoints<DataStream<In>, Out> open ();

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
        JobEndpoints<DataStream<In>, Out> endpoints = this.open();
        DataStream<In> kafkaSource = endpoints.source();
        KafkaSink<Out> sink = endpoints.sink();

        this.execute(kafkaSource, sink);

    }
}
