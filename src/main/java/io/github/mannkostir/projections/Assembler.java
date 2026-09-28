package io.github.mannkostir.projections;

import java.io.Serializable;

public interface Assembler<P, O> extends Serializable {
    O assemble(P parent, Children children) throws Exception;
}
