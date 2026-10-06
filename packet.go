package rapide

import (
	"bytes"
	"encoding/binary"

	"github.com/maklean/rapide/internal/sequence"
)

type Packet struct {
	// raw sequence number for this packet
	RawSequenceNumber uint16

	// the number of bytes in the payload
	PayloadLength uint16

	// the data sent in this packet
	Payload []byte
}

// Decodes a Packet struct from a sequence of bytes.
func FromBytes(data []byte) (*Packet, error) {
	p := &Packet{}
	reader := bytes.NewReader(data)

	err := binary.Read(reader, binary.BigEndian, &p.RawSequenceNumber)
	if err != nil {
		return nil, err
	}

	err = binary.Read(reader, binary.BigEndian, &p.PayloadLength)
	if err != nil {
		return nil, err
	}

	p.Payload = make([]byte, p.PayloadLength)

	err = binary.Read(reader, binary.BigEndian, p.Payload)
	if err != nil {
		return nil, err
	}

	return p, nil
}

// Encodes a Packet struct to a sequence of bytes.
func (p *Packet) ToBytes() ([]byte, error) {
	buf := new(bytes.Buffer)

	if err := binary.Write(buf, binary.BigEndian, p.RawSequenceNumber); err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.BigEndian, p.PayloadLength); err != nil {
		return nil, err
	}

	if _, err := buf.Write(p.Payload); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// Returns the sequenceNumber struct object for the Packet.
func (p *Packet) GetSn() (sn sequence.SequenceNumber) {
	sn = sequence.SnFromValue(p.RawSequenceNumber)
	return
}
