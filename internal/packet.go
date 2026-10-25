package internal

import (
	"bytes"
	"encoding/binary"
)

type Packet struct {
	// the peer connection ID for this packet (0 = unknown)
	ConnectionID uint32

	// whether this is a handshake packet sent by the client or not
	IsHandshakeClient bool

	// whether this is a handshake packet sent by the server of not
	IsHandshakeServer bool
}

// FromBytes parses bytes into a packet.
func FromBytes(data []byte) (*Packet, error) {
	p := &Packet{}

	reader := bytes.NewReader(data)

	for _, field := range []any{&p.ConnectionID, &p.IsHandshakeClient, &p.IsHandshakeServer} {
		if err := binary.Read(reader, binary.BigEndian, field); err != nil {
			return nil, err
		}
	}

	return p, nil
}

// ToBytes serializes p into bytes.
func (p *Packet) ToBytes() ([]byte, error) {
	buf := new(bytes.Buffer)

	for _, field := range []any{p.ConnectionID, p.IsHandshakeClient, p.IsHandshakeServer} {
		if err := binary.Write(buf, binary.BigEndian, field); err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}
