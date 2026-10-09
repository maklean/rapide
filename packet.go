package rapide

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
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

	// whether this is a handshake packet sent by the client or not
	IsHandshakeClient bool

	// whether this is a handshake packet sent by the server of not
	IsHandshakeServer bool
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

	for _, field := range []any{&p.ConnectionID, &p.RawSequenceNumber, &p.PayloadLength} {
		if err := binary.Read(reader, binary.BigEndian, field); err != nil {
			return nil, err
		}
	}

	// safety check to make sure a spoofed datagram doesn't burn through all the memory
	if int64(p.PayloadLength) > int64(reader.Len()) {
		return nil, fmt.Errorf("payload length %d exceeds remaining %d bytes", p.PayloadLength, reader.Len())
	}

	p.Payload = make([]byte, p.PayloadLength)
	if _, err := io.ReadFull(reader, p.Payload); err != nil {
		return nil, err
	}

	for _, field := range []any{&p.IsReliable, &p.IsAck, &p.AckSequenceNumber, &p.IsHandshakeClient, &p.IsHandshakeServer} {
		if err := binary.Read(reader, binary.BigEndian, field); err != nil {
			return nil, err
		}
	}

	return p, nil
}

// Encodes a Packet struct to a sequence of bytes.
func (p *Packet) ToBytes() ([]byte, error) {
	// safety check
	if int64(p.PayloadLength) != int64(len(p.Payload)) {
		return nil, fmt.Errorf("payload length field %d does not match payload size %d", p.PayloadLength, len(p.Payload))
	}

	buf := new(bytes.Buffer)

	for _, field := range []any{p.ConnectionID, p.RawSequenceNumber, p.PayloadLength} {
		if err := binary.Write(buf, binary.BigEndian, field); err != nil {
			return nil, err
		}
	}

	buf.Write(p.Payload)

	for _, field := range []any{p.IsReliable, p.IsAck, p.AckSequenceNumber, p.IsHandshakeClient, p.IsHandshakeServer} {
		if err := binary.Write(buf, binary.BigEndian, field); err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

// Returns the sequenceNumber struct object for the Packet.
func (p *Packet) GetSn() (sn sequence.SequenceNumber) {
	sn = sequence.SnFromValue(p.RawSequenceNumber)
	return
}
