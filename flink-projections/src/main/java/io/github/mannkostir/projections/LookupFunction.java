package io.github.mannkostir.projections;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

final class LookupFunction<E, D, O> extends KeyedCoProcessFunction<String, LookupEntity<E>, Change<D>, Change<O>> {
    private final String name;
    private final TypeInformation<E> entityType;
    private final TypeInformation<D> dimensionType;
    private final LookupOptions options;
    private final LookupRules<E, D, O> rules;
    private transient LookupState<E, D> state;

    LookupFunction(
            String name,
            TypeInformation<E> entityType,
            TypeInformation<D> dimensionType,
            Enricher<E, D, O> enricher,
            LookupOptions options) {
        this.name = name;
        this.entityType = entityType;
        this.dimensionType = dimensionType;
        this.options = options;
        this.rules = new LookupRules<>(enricher, options.requireMatch());
    }

    @Override
    public void open(OpenContext openContext) {
        state = new FlinkLookupState<>(
                getRuntimeContext().getMapState(StateTtl.applyTo(
                        new MapStateDescriptor<>(ContractNames.lookupEntitiesState(name), Types.STRING, entityType),
                        options.stateTtl())),
                getRuntimeContext().getState(StateTtl.applyTo(
                        new ValueStateDescriptor<>(ContractNames.lookupDimensionState(name), dimensionType),
                        options.stateTtl())));
    }

    @Override
    public void processElement1(LookupEntity<E> entity, Context context, Collector<Change<O>> out) throws Exception {
        rules.onEntity(entity, state, out::collect);
    }

    @Override
    public void processElement2(Change<D> change, Context context, Collector<Change<O>> out) throws Exception {
        rules.onDimension(change, state, out::collect);
    }
}
