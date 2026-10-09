package rapide

import (
	"bytes"
	"encoding/binary"
	"time"

	"github.com/maklean/rapide/internal/sequence"
)

type Packet struct {
	// the peer connection ID for this packet (0 = unknown)
	ConnectionID uint32

	// raw sequence number for this packet
	RawSequenceNumber uint16

	// the number of bytes in the payload
	PayloadLength uint16

	// the data sent in this packet
	Payload []byte

	// the sequence number being acknowledged (only relevant if IsAck == true)
	AckSequenceNumber uint16

	// whether this is a reliable (i.e., should send ACK) packet or not
	IsReliable bool

	// whether this is an acknowledgement packet or not
	IsAck bool

	// TODO: rename these to something like IsHandshakeClient/Server
	// whether this is a handshake packet sent by the client or not
	IsConnect bool

	// whether this is a handshake packet sent by the server of not
	IsHandshake bool
}

type unackedPacket struct {
	// Packet struct object (in bytes)
	packet []byte

	// the last time we tried sending this packet
	sentAt time.Time

	// number of times we've tried sending this packet
	retries uint8
}

// Decodes a Packet struct from a sequence of bytes.
func FromBytes(data []byte) (*Packet, error) {
	p := &Packet{}
	reader := bytes.NewReader(data)

	// TODO: clean this up, this can be done inline in the if.
	err := binary.Read(reader, binary.BigEndian, &p.ConnectionID)
	if err != nil {
		return nil, err
	}

	err = binary.Read(reader, binary.BigEndian, &p.RawSequenceNumber)
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

	err = binary.Read(reader, binary.BigEndian, &p.IsReliable)
	if err != nil {
		return nil, err
	}

	err = binary.Read(reader, binary.BigEndian, &p.IsAck)
	if err != nil {
		return nil, err
	}

	err = binary.Read(reader, binary.BigEndian, &p.AckSequenceNumber)
	if err != nil {
		return nil, err
	}

	err = binary.Read(reader, binary.BigEndian, &p.IsConnect)
	if err != nil {
		return nil, err
	}

	err = binary.Read(reader, binary.BigEndian, &p.IsHandshake)
	if err != nil {
		return nil, err
	}

	return p, nil
}

// Encodes a Packet struct to a sequence of bytes.
func (p *Packet) ToBytes() ([]byte, error) {
	buf := new(bytes.Buffer)

	if err := binary.Write(buf, binary.BigEndian, p.ConnectionID); err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.BigEndian, p.RawSequenceNumber); err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.BigEndian, p.PayloadLength); err != nil {
		return nil, err
	}

	if _, err := buf.Write(p.Payload); err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.BigEndian, p.IsReliable); err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.BigEndian, p.IsAck); err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.BigEndian, p.AckSequenceNumber); err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.BigEndian, p.IsConnect); err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.BigEndian, p.IsHandshake); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// Returns the sequenceNumber struct object for the Packet.
func (p *Packet) GetSn() (sn sequence.SequenceNumber) {
	sn = sequence.SnFromValue(p.RawSequenceNumber)
	return
}
