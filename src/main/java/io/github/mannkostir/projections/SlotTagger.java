package io.github.mannkostir.projections;

final class SlotTagger<C> implements ChangeTagger<C, SlotChange> {
    private final int slot;

    SlotTagger(int slot) {
        this.slot = slot;
    }

    @Override
    public SlotChange tag(Routed<C> routed) {
        return new SlotChange(slot, routed.change());
    }
}
