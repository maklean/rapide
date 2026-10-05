package sequence

type SequenceInfo struct {
	// next incoming sequence number
	NextIn SequenceNumber

	// next outgoing sequence number
	NextOut SequenceNumber
}

// Accept returns whether we accepted the given next incoming sequence number
func (s *SequenceInfo) Accept(sn SequenceNumber) bool {
	if s.NextIn.Gt(sn) {
		return false
	}

	s.NextIn = sn.Next()
	return true
}

// AdvanceNextOut returns the current next outgoing sequence number, then moves to the next outgoing sequence number.
func (s *SequenceInfo) AdvanceNextOut() (sn SequenceNumber) {
	sn = s.NextOut
	s.NextOut = s.NextOut.Next()
	return
}
