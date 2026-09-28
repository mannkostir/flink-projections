package io.github.mannkostir.projections;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;

public final class Nest<P> {
    private final String name;
    private final DataStream<Change<P>> parents;
    private final TypeInformation<P> parentType;
    private final List<SlotDefinition<?>> slots = new ArrayList<>();
    private NestOptions options = NestOptions.defaults();
    private boolean assembled;

    private Nest(String name, DataStream<Change<P>> parents, TypeInformation<P> parentType) {
        this.name = name;
        this.parents = parents;
        this.parentType = parentType;
    }

    public static <P> Nest<P> parent(String name, DataStream<Change<P>> parents, TypeInformation<P> type) {
        return new Nest<>(Names.requireValid(name, "Nest name"), parents, type);
    }

    public Nest<P> withOptions(NestOptions options) {
        this.options = Objects.requireNonNull(options, "options");
        return this;
    }

    public <C> ChildSlot<C> child(
            String slot, DataStream<Change<C>> children, KeySelector<C, String> parentKey, TypeInformation<C> type) {
        return child(slot, children, parentKey, type, ChildOptions.defaults());
    }

    public <C> ChildSlot<C> child(
            String slot,
            DataStream<Change<C>> children,
            KeySelector<C, String> parentKey,
            TypeInformation<C> type,
            ChildOptions options) {
        requireNotAssembled();
        requireUniqueSlot(Names.requireValid(slot, "Child slot name"));
        ChildSlot<C> handle = new ChildSlot<>(name, slot, slots.size());
        slots.add(new SlotDefinition<>(handle, children, parentKey, type, Objects.requireNonNull(options, "options")));
        return handle;
    }

    public <O> DataStream<Change<O>> assemble(Assembler<P, O> assembler, TypeInformation<O> type) {
        requireNotAssembled();
        if (slots.isEmpty()) {
            throw new ProjectionConfigurationException(
                    "Nest '" + name + "' has no child slots: call child(...) at least once before assemble(...)");
        }
        assembled = true;
        String uid = ContractNames.nestUid(name);
        return parents.connect(routedChildren())
                .keyBy(new ChangeId<>(), new SlotParentKey(name, slotNames(), parentKeys()), Types.STRING)
                .process(new NestFunction<>(name, parentType, slotSpecs(), assembler, type, options), Changes.typeInfo(type))
                .uid(uid)
                .name(uid);
    }

    private DataStream<SlotChange> routedChildren() {
        SlotChangeTypeInfo slotChangeType = new SlotChangeTypeInfo(slots.stream()
                .<TypeInformation<?>>map(SlotDefinition::type)
                .toList());
        List<DataStream<SlotChange>> routed = slots.stream()
                .map(slot -> slot.route(name, slotChangeType))
                .toList();
        DataStream<SlotChange> first = routed.get(0);
        return routed.size() == 1 ? first : first.union(routed.subList(1, routed.size()).toArray(DataStream[]::new));
    }

    private List<String> slotNames() {
        return slots.stream().map(slot -> slot.handle().name()).toList();
    }

    private List<KeySelector<Object, String>> parentKeys() {
        return slots.stream().map(SlotDefinition::erasedParentKey).toList();
    }

    private List<SlotSpec> slotSpecs() {
        return slots.stream().map(slot -> new SlotSpec(slot.handle().name(), slot.type(), slot.options())).toList();
    }

    private void requireNotAssembled() {
        if (assembled) {
            throw new ProjectionConfigurationException(
                    "Nest '" + name + "' is already assembled: build a new Nest for another level");
        }
    }

    private void requireUniqueSlot(String slot) {
        if (slots.stream().anyMatch(existing -> existing.handle().name().equals(slot))) {
            throw new ProjectionConfigurationException(
                    "Nest '" + name + "' already has a child slot named '" + slot + "': slot names must be unique within a level");
        }
    }

    private record SlotDefinition<C>(
            ChildSlot<C> handle,
            DataStream<Change<C>> stream,
            KeySelector<C, String> parentKey,
            TypeInformation<C> type,
            ChildOptions options) {

        DataStream<SlotChange> route(String level, SlotChangeTypeInfo slotChangeType) {
            String uid = ContractNames.nestRouteUid(level, handle.name());
            RoutingFunction<C, SlotChange> routing = new RoutingFunction<>(
                    ContractNames.nestRouteState(level, handle.name()),
                    parentKey,
                    type,
                    options.stateTtl(),
                    new SlotTagger<>(handle.index()));
            return stream.keyBy(new ChangeId<>(), Types.STRING)
                    .process(routing, slotChangeType)
                    .uid(uid)
                    .name(uid);
        }

        @SuppressWarnings("unchecked")
        KeySelector<Object, String> erasedParentKey() {
            return (KeySelector<Object, String>) (KeySelector<?, String>) parentKey;
        }
    }
}
