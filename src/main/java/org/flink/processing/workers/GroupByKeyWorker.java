package org.flink.processing.workers;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.flink.processing.functions.GroupByKeyFunction;
import org.flink.processing.jobs.DataStreamWithKey;
import org.flink.processing.records.RecordsGroup;
import org.flink.processing.records.interfaces.DeletableRecord;
import org.flink.processing.records.interfaces.IdentifiableRecord;
import org.flink.processing.records.interfaces.KeyedRecord;
import org.flink.processing.workers.base.Worker;

public class GroupByKeyWorker<T extends IdentifiableRecord & DeletableRecord & KeyedRecord>
        extends Worker<DataStream<RecordsGroup<T>>> {
    private final DataStreamWithKey<T> streamSource;
    private final Class<T> outClass;

    public GroupByKeyWorker (DataStreamWithKey<T> streamSource) {
        super();
        this.streamSource = streamSource;
        this.outClass = this.streamSource.stream().getType().getTypeClass();
    }

    @Override
    public DataStream<RecordsGroup<T>> run (String processName) {
        return this.streamSource.stream()
                                .keyBy(this.streamSource.keySelector())
                                .process(new GroupByKeyFunction<>(this.outClass.getName(),
                                                                  this.outClass
                                ))
                                .name("group-by-key_" + processName)
                                .uid("group-by-key_" + processName);
    }
}
