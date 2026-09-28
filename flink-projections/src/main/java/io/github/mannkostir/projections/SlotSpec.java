package io.github.mannkostir.projections;

import java.io.Serializable;

import org.apache.flink.api.common.typeinfo.TypeInformation;

record SlotSpec(String name, TypeInformation<?> valueType, ChildOptions options) implements Serializable {
}
