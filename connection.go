package rapide

import (
	"log"
	"sync"

	"github.com/maklean/rapide/internal/config"
	"github.com/maklean/rapide/internal/sequence"
	"github.com/maklean/rapide/internal/structures"
	"github.com/maklean/rapide/internal/writer"
)

type connection struct {
	// packet writer for this connection
	writer writer.PacketWriter

	// sequence number information for connection
	sequenceInfo sequence.SequenceInfo

	// queue of packets waiting to be acknowledged
	ackQueue *structures.RingBuffer[unackedPacket]

	// mutex for the ackQueue
	ackQueueMu sync.Mutex

	// incoming packets channel for connection
	incomingPackets chan *Packet
}

// Send sends a byte payload to the peer address of this connection
func (c *connection) Send(payload []byte) error {
	packet := Packet{
		RawSequenceNumber: c.sequenceInfo.AdvanceOut().Value,
		PayloadLength:     uint16(len(payload)),
		Payload:           payload,
	}

	data, err := packet.ToBytes()
	if err != nil {
		return err
	}

	return c.writer.Write(data)
}

// newConnection creates a new connection struct instance who's connected to the given peer address.
func newConnection(writer writer.PacketWriter) *connection {
	conn := &connection{
		writer:          writer,
		sequenceInfo:    sequence.NewSequenceInfo(),
		ackQueue:        structures.NewRingBuffer[unackedPacket](),
		incomingPackets: make(chan *Packet, config.MaxIncomingPackets), // TODO: should probably not use a magic number for this
	}

	// connection loop - handles incoming packets for connection
	go func() {
		for p := range conn.incomingPackets {
			conn.handlePacket(p)
		}
	}()

	return conn
}

// deliver adds the given packet to the connections incoming packets channel.
func (c *connection) deliver(p *Packet) {
	select {
	case c.incomingPackets <- p:
	default:
	}
}

// handlePacket handles the given packet.
func (c *connection) handlePacket(p *Packet) {
	log.Printf("Received payload: '%s' (%d bytes)", string(p.Payload), p.PayloadLength)
}
