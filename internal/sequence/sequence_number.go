package sequence

type SequenceNumber struct {
	Value uint16
}

// snFromValue creates a sequenceNumber struct object from a given value.
func SnFromValue(value uint16) (sn SequenceNumber) {
	sn.Value = value
	return
}

// Next returns the next sequence number from the current one.
func (sn SequenceNumber) Next() (sn_n SequenceNumber) {
	sn_n.Value = sn.Value + 1
	return
}

// Le returns whether the current sequence number is less than the given one.
func (sn SequenceNumber) Le(other SequenceNumber) bool {
	return sn.distance(other) < 0
}

// Gt returns whether the current sequence number is greater than the given one.
func (sn SequenceNumber) Gt(other SequenceNumber) bool {
	return sn.distance(other) > 0
}

// Leq returns whether the current sequence number is less than or equal than the given one.
func (sn SequenceNumber) Leq(other SequenceNumber) bool {
	return sn.distance(other) <= 0
}

// Geq returns whether the current sequence number is greater than or equal than the given one.
func (sn SequenceNumber) Geq(other SequenceNumber) bool {
	return sn.distance(other) >= 0
}

// Eq returns whether the current sequence number is equal to the given one.
func (sn SequenceNumber) Eq(other SequenceNumber) bool {
	return sn.distance(other) == 0
}

// distance returns the signed distance between the current and given sequence number.
func (sn SequenceNumber) distance(other SequenceNumber) int16 {
	var diff uint16 = sn.Value - other.Value
	return int16(diff)
}
