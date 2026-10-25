package internal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

type Packet struct {
	// the peer connection ID for this packet
	ConnectionID uint32

	// whether this is a handshake packet sent by the client or not
	IsHandshakeClient bool

	// whether this is a handshake packet sent by the server of not
	IsHandshakeServer bool

	// size of data being sent
	PayloadSize uint16

	// data being sent
	Payload []byte
}

// FromBytes parses bytes into a packet.
func FromBytes(data []byte) (*Packet, error) {
	p := &Packet{}

	reader := bytes.NewReader(data)

	for _, field := range []any{&p.ConnectionID, &p.IsHandshakeClient, &p.IsHandshakeServer, &p.PayloadSize} {
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

	for _, field := range []any{p.ConnectionID, p.IsHandshakeClient, p.IsHandshakeServer, p.PayloadSize} {
		if err := binary.Write(buf, binary.BigEndian, field); err != nil {
			return nil, err
		}
	}

	buf.Write(p.Payload)

	return buf.Bytes(), nil
}
