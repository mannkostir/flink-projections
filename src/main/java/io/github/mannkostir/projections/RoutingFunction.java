package io.github.mannkostir.projections;

import java.time.Duration;
import java.util.Optional;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

final class RoutingFunction<T, R> extends KeyedProcessFunction<String, Change<T>, R> {
    private final String stateName;
    private final TypeInformation<T> valueType;
    private final Duration stateTtl;
    private final RoutingRules<T> rules;
    private final ChangeTagger<T, R> tagger;
    private transient ValueState<T> lastValue;

    RoutingFunction(
            String stateName,
            KeySelector<T, String> targetKey,
            TypeInformation<T> valueType,
            Optional<Duration> stateTtl,
            ChangeTagger<T, R> tagger) {
        this.stateName = stateName;
        this.valueType = valueType;
        this.stateTtl = stateTtl.orElse(null);
        this.rules = new RoutingRules<>(targetKey);
        this.tagger = tagger;
    }

    @Override
    public void open(OpenContext openContext) {
        ValueStateDescriptor<T> descriptor = new ValueStateDescriptor<>(stateName, valueType);
        lastValue = getRuntimeContext().getState(StateTtl.applyTo(descriptor, Optional.ofNullable(stateTtl)));
    }

    @Override
    public void processElement(Change<T> change, Context context, Collector<R> out) throws Exception {
        for (Routed<T> routed : rules.route(lastValue.value(), change)) {
            out.collect(tagger.tag(routed));
        }
        remember(change);
    }

    private void remember(Change<T> change) throws Exception {
        if (change instanceof Delete<T>) {
            lastValue.clear();
        } else {
            lastValue.update(change.value());
        }
    }
}
