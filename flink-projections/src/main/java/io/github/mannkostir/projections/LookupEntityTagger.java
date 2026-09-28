package io.github.mannkostir.projections;

final class LookupEntityTagger<E> implements ChangeTagger<E, LookupEntity<E>> {
    @Override
    public LookupEntity<E> tag(Routed<E> routed) {
        return new LookupEntity<>(routed.change(), routed.relocation());
    }
}
