package sequence

import "sync"

type SequenceInfo struct {
	// next incoming sequence number
	NextIn SequenceNumber

	// next outgoing sequence number
	NextOut SequenceNumber

	// NextOut mutex
	nextOutMu sync.Mutex
}

// NewSequenceInfo creates a new SequenceInfo struct object
func NewSequenceInfo() *SequenceInfo {
	return &SequenceInfo{NextIn: SnFromValue(0), NextOut: SnFromValue(0)}
}

// Accept returns whether we accepted the given next incoming sequence number
func (s *SequenceInfo) Accept(sn SequenceNumber) bool {
	if s.NextIn.Gt(sn) {
		return false
	}

	s.NextIn = sn.Next()
	return true
}

// AdvanceOut returns the current next outgoing sequence number, then moves to the next outgoing sequence number.
func (s *SequenceInfo) AdvanceOut() (sn SequenceNumber) {
	s.nextOutMu.Lock()
	defer s.nextOutMu.Unlock()

	sn = s.NextOut
	s.NextOut = s.NextOut.Next()
	return
}
