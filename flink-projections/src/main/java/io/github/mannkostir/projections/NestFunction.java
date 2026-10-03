package io.github.mannkostir.projections;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

final class NestFunction<P, O> extends KeyedCoProcessFunction<String, Change<P>, SlotChange, Change<O>> {
    private final String level;
    private final TypeInformation<P> parentType;
    private final TypeInformation<O> docType;
    private final List<SlotSpec> slots;
    private final NestOptions options;
    private final NestRules<P, O> rules;
    private transient LevelState<P, O> state;

    NestFunction(
            String level,
            TypeInformation<P> parentType,
            List<SlotSpec> slots,
            Assembler<P, O> assembler,
            TypeInformation<O> docType,
            NestOptions options) {
        this.level = level;
        this.parentType = parentType;
        this.docType = docType;
        this.slots = List.copyOf(slots);
        this.options = options;
        this.rules = new NestRules<>(level, slots.size(), assembler);
    }

    @Override
    public void open(OpenContext openContext) {
        state = new FlinkLevelState<>(
                getRuntimeContext().getState(StateTtl.applyTo(
                        new ValueStateDescriptor<>(ContractNames.nestParentState(level), parentType),
                        options.parentStateTtl())),
                childStates(),
                getRuntimeContext().getState(StateTtl.applyTo(
                        new ValueStateDescriptor<>(ContractNames.nestLastDocState(level), docType),
                        options.parentStateTtl())));
    }

    @Override
    public void processElement1(Change<P> change, Context context, Collector<Change<O>> out) throws Exception {
        rules.onParent(context.getCurrentKey(), change, state, out::collect);
    }

    @Override
    public void processElement2(SlotChange change, Context context, Collector<Change<O>> out) throws Exception {
        rules.onChild(context.getCurrentKey(), change, state, out::collect);
        if (change.change() instanceof Upsert<?> && state.parent() == null) {
            scheduleOrphanCheck(context);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext context, Collector<Change<O>> out) throws Exception {
        rules.onOrphanTimeout(state);
    }

    private void scheduleOrphanCheck(Context context) {
        options.orphanTimeout().map(OrphanDeadline::new)
                .ifPresent(deadline -> rescheduleOrphanCheck(context.timerService(), context.getCurrentKey(), deadline));
    }

    private static void rescheduleOrphanCheck(TimerService timers, String key, OrphanDeadline orphanDeadline) {
        long deadline = orphanDeadline.after(key, timers.currentProcessingTime());
        orphanDeadline.superseded(deadline).forEach(timers::deleteProcessingTimeTimer);
        timers.registerProcessingTimeTimer(deadline);
    }

    @SuppressWarnings("unchecked")
    private List<MapState<String, Object>> childStates() {
        List<MapState<String, Object>> childStates = new ArrayList<>(slots.size());
        for (SlotSpec slot : slots) {
            MapStateDescriptor<String, Object> descriptor = new MapStateDescriptor<>(
                    ContractNames.nestChildState(level, slot.name()),
                    Types.STRING,
                    (TypeInformation<Object>) slot.valueType());
            childStates.add(getRuntimeContext().getMapState(StateTtl.applyTo(descriptor, slot.options().stateTtl())));
        }
        return childStates;
    }
}
