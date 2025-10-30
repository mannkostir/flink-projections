package org.flink.processing.functions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.stream.Collectors;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.flink.processing.records.RecordsGroup;
import org.flink.processing.records.interfaces.DeletableRecord;
import org.flink.processing.records.interfaces.IdentifiableRecord;
import org.flink.processing.records.interfaces.KeyedRecord;

public class GroupByKeyFunction<T extends IdentifiableRecord & DeletableRecord & KeyedRecord>
        extends KeyedProcessFunction<String, T, RecordsGroup<T>> {
    private final Class<T> outClass;
    private final TypeSerializer<T> typeSerializer;
    private final String name;
    ListState<T> state;

    public GroupByKeyFunction (String name, Class<T> outClass) {
        this.outClass = outClass;
        this.typeSerializer = null;
        this.name = name;
    }

    public GroupByKeyFunction (String name, TypeSerializer<T> typeSerializer) {
        this.outClass = null;
        this.typeSerializer = typeSerializer;
        this.name = name;
    }

    @Override
    public void open (OpenContext openContext) {
        if (this.typeSerializer != null) {
            ListStateDescriptor<T> desc = new ListStateDescriptor<>(this.name, this.typeSerializer);

            state = getRuntimeContext().getListState(desc);
        }

        if (this.outClass != null) {
            ListStateDescriptor<T> desc = new ListStateDescriptor<>(this.name, this.outClass);

            state = getRuntimeContext().getListState(desc);
        }
    }

    private RecordsGroup<T> getState (T record, String key)
    throws Exception {
        ArrayList<T> localState = new ArrayList<>(Collections.singletonList(record));

        try {
            Iterable<T> savedValues = this.state.get();

            if (savedValues != null) {
                for (T savedValue : savedValues) {
                    if (savedValue.recordId().equals(record.recordId())) {
                        continue;
                    }
                    localState.add(savedValue);
                }
            }
        } catch (Exception e) {
            // If there's a deserialization error, log it and continue with just the current record
            System.err.println("Error deserializing state: " + e.getMessage() + ". Continuing with empty state.");
            e.printStackTrace();
        }

        return new RecordsGroup<>(key, localState);
    }

    @Override
    public void processElement (
            T record,
            KeyedProcessFunction<String, T, RecordsGroup<T>>.Context ctx,
            Collector<RecordsGroup<T>> out
    )
    throws Exception {
        RecordsGroup<T> localUpdatedState = this.getState(record, ctx.getCurrentKey());

        if (record.isDeleted()) {
            ArrayList<T> filteredRecords = localUpdatedState.records.stream()
                                                                    .filter(s -> !s.recordId()
                                                                                   .equals(record.recordId()))
                                                                    .collect(Collectors.toCollection(
                                                                            ArrayList::new))
                    ;

            this.state.update(filteredRecords);

            out.collect(new RecordsGroup<>(ctx.getCurrentKey(), filteredRecords));
        } else {
            this.state.update(localUpdatedState.records);

            out.collect(localUpdatedState);
        }
    }
}
