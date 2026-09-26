package io.github.mannkostir.flink.processing.workers;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import io.github.mannkostir.flink.processing.jobs.DataStreamWithKey;
import io.github.mannkostir.flink.processing.jobs.interfaces.ICreateEnrichedRecords;
import io.github.mannkostir.flink.processing.records.EnrichedRecord;
import io.github.mannkostir.flink.processing.records.interfaces.CommonRecord;
import io.github.mannkostir.flink.processing.records.interfaces.DeletableRecord;
import io.github.mannkostir.flink.processing.records.interfaces.IdentifiableRecord;
import io.github.mannkostir.flink.processing.workers.base.Worker;

class EnrichStreamFunction<Source extends IdentifiableRecord & DeletableRecord, Connected extends DeletableRecord, Output extends EnrichedRecord>
        extends CoProcessFunction<Source, Connected, Output> {
    private final Class<Source> sourceClass;
    private final Class<Connected> connectedClass;
    private final ICreateEnrichedRecords<Source, Connected, Output> recordsFactory;
    private MapState<String, Source> sourceState;
    private ValueState<Connected> connectedState;

    public EnrichStreamFunction (
            Class<Source> sourceClass,
            Class<Connected> connectedClass,
            ICreateEnrichedRecords<Source, Connected, Output> recordsFactory
    ) {
        this.sourceClass = sourceClass;
        this.connectedClass = connectedClass;
        this.recordsFactory = recordsFactory;
    }

    @Override
    public void open (OpenContext openContext) {
        MapStateDescriptor<String, Source> sourceStateDescriptor
                = new MapStateDescriptor<>(this.sourceClass.getName(),
                                           String.class,
                                           this.sourceClass
        );

        this.sourceState = getRuntimeContext().getMapState(sourceStateDescriptor);

        ValueStateDescriptor<Connected> connectedStateDescriptor
                = new ValueStateDescriptor<>(this.connectedClass.getName(), this.connectedClass);

        this.connectedState = getRuntimeContext().getState(connectedStateDescriptor);
    }

    @Override
    public void processElement1 (
            Source value,
            CoProcessFunction<Source, Connected, Output>.Context ctx,
            Collector<Output> out
    )
    throws Exception {
        Output enrichedRecord;

        if (value.isDeleted()) {
            this.sourceState.remove(value.recordId());
            enrichedRecord = this.recordsFactory.createEnrichedRecord(value, null);

        } else {
            this.sourceState.put(value.recordId(), value);

            enrichedRecord = this.recordsFactory.createEnrichedRecord(value,
                                                                      this.connectedState.value()
            );

        }

        if (enrichedRecord.isReady()) {
            out.collect(enrichedRecord);
        }
    }

    @Override
    public void processElement2 (
            Connected value,
            CoProcessFunction<Source, Connected, Output>.Context ctx,
            Collector<Output> out
    )
    throws Exception {
        if (this.sourceState.isEmpty()) {
            this.connectedState.update(value);
            return;
        }

        if (value.isDeleted()) {
            for (Source source : this.sourceState.values()) {
                Output enrichedRecord = this.recordsFactory.createEnrichedRecord(source, null);

                if (enrichedRecord.isReady()) {
                    out.collect(enrichedRecord);
                }
            }

            this.connectedState.clear();
            this.sourceState.clear();
        } else {
            this.connectedState.update(value);

            for (Source source : this.sourceState.values()) {
                Output enrichedRecord = this.recordsFactory.createEnrichedRecord(source, value);

                if (enrichedRecord.isReady()) {
                    out.collect(enrichedRecord);
                }
            }
        }
    }
}

public class EnrichStreamWorker<Source extends CommonRecord & IdentifiableRecord & DeletableRecord, Connected extends CommonRecord & DeletableRecord, Output extends EnrichedRecord>
        extends Worker<DataStream<Output>> {
    private final DataStreamWithKey<Source> sourceStream;
    private final DataStreamWithKey<Connected> connectedStream;
    private final ICreateEnrichedRecords<Source, Connected, Output> recordsFactory;
    private final CoProcessFunction<Source, Connected, Output> connectedProcessFunction;

    public EnrichStreamWorker (
            DataStreamWithKey<Source> sourceStream,
            DataStreamWithKey<Connected> connectedStream,
            ICreateEnrichedRecords<Source, Connected, Output> recordsFactory,
            CoProcessFunction<Source, Connected, Output> connectedProcessFunction
    ) {
        this.sourceStream = sourceStream;
        this.connectedStream = connectedStream;
        this.recordsFactory = recordsFactory;
        this.connectedProcessFunction = connectedProcessFunction;
    }

    public EnrichStreamWorker (
            DataStreamWithKey<Source> sourceStream,
            DataStreamWithKey<Connected> connectedStream,
            ICreateEnrichedRecords<Source, Connected, Output> recordsFactory
    ) {
        this.sourceStream = sourceStream;
        this.connectedStream = connectedStream;
        this.recordsFactory = recordsFactory;
        this.connectedProcessFunction = new EnrichStreamFunction<>(this.sourceStream.sourceClass(),
                                                                   this.connectedStream.sourceClass(),
                                                                   this.recordsFactory
        );
    }

    public DataStream<Output> run (String processName) {
        ConnectedStreams<Source, Connected> streams = this.sourceStream.stream()
                                                                       .connect(this.connectedStream.stream())
                                                                       .keyBy(
                                                                               this.sourceStream.keySelector(),
                                                                               this.connectedStream.keySelector()
                                                                       )
                ;

        return streams.process(this.connectedProcessFunction,
                               this.recordsFactory.enrichedRecordTypeInformation()
                      )
                      .name("enriched-stream_" + processName)
                      .uid("enriched-stream_" + processName);
    }
}
