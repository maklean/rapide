package rapide

import (
	"bytes"
	"encoding/binary"

	"github.com/maklean/rapide/internal/sequence"
)

type Packet struct {
	// raw sequence number for this packet
	RawSequenceNumber uint16

	// the data sent in this packet
	Payload []byte
}

// Decodes a Packet struct from a sequence of bytes.
func FromBytes(data []byte) (p *Packet, err error) {
	err = binary.Read(bytes.NewReader(data), binary.BigEndian, p)
	return
}

// Encodes a Packet struct to a sequence of bytes.
func (p *Packet) ToBytes() ([]byte, error) {
	buf := new(bytes.Buffer)

	err := binary.Write(buf, binary.BigEndian, p)
	if err != nil {
		return nil, err
	}

	data := buf.Bytes()
	return data, nil
}

// Returns the sequenceNumber struct object for the Packet.
func (p *Packet) GetSn() (sn sequence.SequenceNumber) {
	sn = sequence.SnFromValue(p.RawSequenceNumber)
	return
}
