package internal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"time"
)

// PacketHeaderSize is the sum of the size of all struct members (excluding Payload)
const PacketHeaderSize = 4 + 2 + 2 + 1 + 1 + 1 + 1 + 2

type Packet struct {
	// peer connection ID
	ConnectionID uint32

	// sequence number
	RawSequenceNumber uint16

	// sequence number being acknowledged
	AckSequenceNumber uint16

	// whether this is a handshake packet sent by the client or not
	IsHandshakeClient bool

	// whether this is a handshake packet sent by the server of not
	IsHandshakeServer bool

	// whether this packet is reliable or not
	IsReliable bool

	// whether this packet is an acknowledgement packet or not
	IsAck bool

	// size of data being sent
	PayloadSize uint16

	// data being sent
	Payload []byte
}

type UnackedPacket struct {
	// serialized packet
	Packet []byte

	// most recent send
	LastSend time.Time

	// number of times we've tried retransmitting
	Retries uint8
}

// FromBytes parses bytes into a packet.
func FromBytes(data []byte) (*Packet, error) {
	p := &Packet{}

	reader := bytes.NewReader(data)

	for _, field := range []any{&p.ConnectionID, &p.RawSequenceNumber, &p.AckSequenceNumber, &p.IsHandshakeClient, &p.IsHandshakeServer, &p.IsReliable, &p.IsAck, &p.PayloadSize} {
		if err := binary.Read(reader, binary.BigEndian, field); err != nil {
			return nil, err
		}
	}

	if uint64(p.PayloadSize) != uint64(reader.Len()) {
		return nil, fmt.Errorf("%w: expected size of %d, got %d", ErrPayloadSizeMismatch, p.PayloadSize, reader.Len())
	}

	p.Payload = make([]byte, p.PayloadSize)
	if _, err := io.ReadFull(reader, p.Payload); err != nil {
		return nil, err
	}

	return p, nil
}

// ToBytes serializes p into bytes.
func (p *Packet) ToBytes() ([]byte, error) {
	if uint64(p.PayloadSize) != uint64(len(p.Payload)) {
		return nil, fmt.Errorf("%w: expected size of %d, got %d", ErrPayloadSizeMismatch, p.PayloadSize, len(p.Payload))
	}

	buf := new(bytes.Buffer)

	for _, field := range []any{p.ConnectionID, p.RawSequenceNumber, p.AckSequenceNumber, p.IsHandshakeClient, p.IsHandshakeServer, p.IsReliable, p.IsAck, p.PayloadSize} {
		if err := binary.Write(buf, binary.BigEndian, field); err != nil {
			return nil, err
		}
	}

	buf.Write(p.Payload)

	return buf.Bytes(), nil
}

// Sn returns the Packet's SequenceNumber.
func (p *Packet) Sn() SequenceNumber {
	return SequenceNumberInit(p.RawSequenceNumber)
}
