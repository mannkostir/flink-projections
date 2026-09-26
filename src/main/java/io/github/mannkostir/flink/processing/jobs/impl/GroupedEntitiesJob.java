package io.github.mannkostir.flink.processing.jobs.impl;

import java.util.ArrayList;
import java.util.stream.Collectors;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import io.github.mannkostir.flink.io.kafka.config.KafkaClientConfig;
import io.github.mannkostir.flink.processing.jobs.JobEndpoints;
import io.github.mannkostir.flink.processing.jobs.DataStreamWithKey;
import io.github.mannkostir.flink.processing.jobs.base.Job;
import io.github.mannkostir.flink.processing.jobs.interfaces.ICreateTransformedGroupedRecords;
import io.github.mannkostir.flink.processing.records.RecordsGroup;
import io.github.mannkostir.flink.processing.records.SinkRecord;
import io.github.mannkostir.flink.processing.records.interfaces.CommonRecord;
import io.github.mannkostir.flink.processing.records.interfaces.DeletableRecord;
import io.github.mannkostir.flink.processing.records.interfaces.IdentifiableRecord;
import io.github.mannkostir.flink.processing.records.interfaces.KeyedRecord;
import io.github.mannkostir.flink.processing.workers.GroupByKeyWorker;

class GroupedToSinkMapFunction<In extends CommonRecord, Out extends CommonRecord, Group extends CommonRecord>
        implements MapFunction<RecordsGroup<In>, Group> {
    private final ICreateTransformedGroupedRecords<In, Out, Group> recordsFactory;

    public GroupedToSinkMapFunction (ICreateTransformedGroupedRecords<In, Out, Group> recordsFactory) {
        this.recordsFactory = recordsFactory;
    }

    public Group map (RecordsGroup<In> value) {
        ArrayList<Out> records = value.records.stream()
                                              .map(this.recordsFactory::transformRecord)
                                              .collect(Collectors.toCollection(ArrayList::new))
                ;

        return recordsFactory.createGroupRecord(new RecordsGroup<>(value.key, records));
    }
}

public abstract class GroupedEntitiesJob<In extends IdentifiableRecord & DeletableRecord & KeyedRecord, OutPayload extends SpecificRecordBase, Out extends SinkRecord<OutPayload>, GroupPayload extends SpecificRecordBase, Group extends SinkRecord<GroupPayload>>
        extends Job {
    protected final ICreateTransformedGroupedRecords<In, Out, Group> recordsFactory;

    protected GroupedEntitiesJob (
            ICreateTransformedGroupedRecords<In, Out, Group> recordsFactory, String name, KafkaClientConfig config
    ) {
        super(name, config);
        this.recordsFactory = recordsFactory;
    }

    protected abstract JobEndpoints<DataStreamWithKey<In>, Group> open ();

    private void execute (DataStreamWithKey<In> streamSource, KafkaSink<Group> sink) {
        DataStream<RecordsGroup<In>> groupedRecordsStream
                = new GroupByKeyWorker<>(streamSource).run(this.jobName);

        DataStream<Group> groupedRecordsSinkStream
                = groupedRecordsStream.map(new GroupedToSinkMapFunction<>(this.recordsFactory))
                                      .name("grouped-to-sink_" + this.jobName)
                                      .uid("grouped-to-sink_" + this.jobName)
                                      .returns(this.recordsFactory.groupedRecordTypeInformation())
                ;

        groupedRecordsSinkStream.sinkTo(sink)
                                .name("sink-grouped_" + this.jobName)
                                .uid("sink-grouped_" + this.jobName)
        ;
    }

    @Override
    public final void run () {
        JobEndpoints<DataStreamWithKey<In>, Group> endpoints = this.open();
        DataStreamWithKey<In> kafkaSource = endpoints.source();
        KafkaSink<Group> sink = endpoints.sink();

        this.execute(kafkaSource, sink);

    }
}
