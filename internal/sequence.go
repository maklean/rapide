package internal

type Sequence struct {
	// next incoming sequence number
	NextIn SequenceNumber

	// next outgoing sequence number
	NextOut SequenceNumber
}

// NewSequence creates a zero-initialized pack of sequence numbers.
func NewSequence() (s Sequence) {
	s.NextIn = SequenceNumberInit(0)
	s.NextOut = SequenceNumberInit(0)
	return
}
