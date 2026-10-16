package sequence

type SequencePack struct {
	// next incoming sequence number for this pack
	NextIn SequenceNumber

	// next outgoing outgoing sequence number for this pack
	NextOut SequenceNumber
}

// NewSequencePack zero-initialized creates a new SequencePack.
func NewSequencePack() (p SequencePack) {
	p.NextIn = SnFromValue(0)
	p.NextOut = SnFromValue(0)
	return
}
