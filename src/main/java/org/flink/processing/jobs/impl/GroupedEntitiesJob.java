package org.flink.processing.jobs.impl;

import java.util.ArrayList;
import java.util.stream.Collectors;

import javax.swing.GroupLayout.Group;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.commons.math3.util.Pair;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.flink.io.kafka.config.KafkaClientConfig;
import org.flink.processing.jobs.DataStreamWithKey;
import org.flink.processing.jobs.base.Job;
import org.flink.processing.jobs.interfaces.ICreateTransformedGroupedRecords;
import org.flink.processing.records.RecordsGroup;
import org.flink.processing.records.SinkRecord;
import org.flink.processing.records.interfaces.CommonRecord;
import org.flink.processing.records.interfaces.DeletableRecord;
import org.flink.processing.records.interfaces.IdentifiableRecord;
import org.flink.processing.records.interfaces.KeyedRecord;
import org.flink.processing.workers.GroupByKeyWorker;

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

    protected abstract Pair<DataStreamWithKey<In>, KafkaSink<Group>> open ();

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
        Pair<DataStreamWithKey<In>, KafkaSink<Group>> pair = this.open();
        DataStreamWithKey<In> kafkaSource = pair.getKey();
        KafkaSink<Group> sink = pair.getValue();

        this.execute(kafkaSource, sink);

    }
}
