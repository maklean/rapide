package internal

// Sequence number following serial number arithmetic (RFC 1982)
type SequenceNumber struct {
	Value uint16
}

// NewSequenceNumber initializes a SequenceNumber with the given value
func NewSequenceNumber(value uint16) (sn SequenceNumber) {
	sn.Value = value
	return
}

// Next returns the next sequence number from the current one.
func (sn SequenceNumber) Next() (snN SequenceNumber) {
	snN.Value = sn.Value + 1
	return
}

// Lt returns whether the current sequence number is less than the given one.
func (sn SequenceNumber) Lt(other SequenceNumber) bool {
	return sn.Distance(other) < 0
}

// Gt returns whether the current sequence number is greater than the given one.
func (sn SequenceNumber) Gt(other SequenceNumber) bool {
	return sn.Distance(other) > 0
}

// Leq returns whether the current sequence number is less than or equal than the given one.
func (sn SequenceNumber) Leq(other SequenceNumber) bool {
	return sn.Distance(other) <= 0
}

// Geq returns whether the current sequence number is greater than or equal than the given one.
func (sn SequenceNumber) Geq(other SequenceNumber) bool {
	return sn.Distance(other) >= 0
}

// Eq returns whether the current sequence number is equal to the given one.
func (sn SequenceNumber) Eq(other SequenceNumber) bool {
	return sn.Distance(other) == 0
}

// Distance returns the signed distance between the current and given sequence number.
func (sn SequenceNumber) Distance(other SequenceNumber) int16 {
	var diff uint16 = sn.Value - other.Value
	return int16(diff)
}
