package io.github.mannkostir.projections;

import java.util.Objects;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;

public final class Lookup<E> {
    private final String name;
    private final DataStream<Change<E>> entities;
    private final KeySelector<E, String> lookupKey;
    private final TypeInformation<E> entityType;
    private LookupOptions options = LookupOptions.defaults();

    private Lookup(String name, DataStream<Change<E>> entities, KeySelector<E, String> lookupKey, TypeInformation<E> entityType) {
        this.name = name;
        this.entities = entities;
        this.lookupKey = lookupKey;
        this.entityType = entityType;
    }

    public static <E> Lookup<E> of(
            String name, DataStream<Change<E>> entities, KeySelector<E, String> lookupKey, TypeInformation<E> type) {
        return new Lookup<>(Names.requireValid(name, "Lookup name"), entities, lookupKey, type);
    }

    public Lookup<E> withOptions(LookupOptions options) {
        this.options = Objects.requireNonNull(options, "options");
        return this;
    }

    public <D> WithDimension<E, D> from(DataStream<Change<D>> dimensions, TypeInformation<D> type) {
        return new WithDimension<>(this, dimensions, type);
    }

    public static final class WithDimension<E, D> {
        private final Lookup<E> lookup;
        private final DataStream<Change<D>> dimensions;
        private final TypeInformation<D> dimensionType;

        private WithDimension(Lookup<E> lookup, DataStream<Change<D>> dimensions, TypeInformation<D> dimensionType) {
            this.lookup = lookup;
            this.dimensions = dimensions;
            this.dimensionType = dimensionType;
        }

        public <O> DataStream<Change<O>> enrich(Enricher<E, D, O> enricher, TypeInformation<O> type) {
            String uid = ContractNames.lookupUid(lookup.name);
            return routedEntities()
                    .connect(dimensions)
                    .keyBy(new LookupEntityKey<>(lookup.name, lookup.lookupKey), new ChangeId<>(), Types.STRING)
                    .process(new LookupFunction<>(lookup.name, lookup.entityType, dimensionType, enricher, lookup.options),
                            Changes.typeInfo(type))
                    .uid(uid)
                    .name(uid);
        }

        private DataStream<LookupEntity<E>> routedEntities() {
            String uid = ContractNames.lookupRouteUid(lookup.name);
            RoutingFunction<E, LookupEntity<E>> routing = new RoutingFunction<>(
                    ContractNames.lookupRouteState(lookup.name),
                    lookup.lookupKey,
                    lookup.entityType,
                    lookup.options.stateTtl(),
                    new LookupEntityTagger<>());
            return lookup.entities.keyBy(new ChangeId<>(), Types.STRING)
                    .process(routing, new LookupEntityTypeInfo<>(lookup.entityType))
                    .uid(uid)
                    .name(uid);
        }
    }
}
