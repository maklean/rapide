package rapide

import (
	"log"
	"net"
	"sync"

	"github.com/maklean/rapide/internal/config"
	"github.com/maklean/rapide/internal/sequence"
	"github.com/maklean/rapide/internal/structures"
	"github.com/maklean/rapide/internal/writer"
)

type connection struct {
	// socket for this connection
	socket *net.UDPConn

	// packet writer for this connection
	writer writer.PacketWriter

	// sequence number information for connection
	sequenceInfo *sequence.SequenceInfo

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
func newConnection(socket *net.UDPConn, writer writer.PacketWriter) *connection {
	conn := &connection{
		socket:          socket,
		writer:          writer,
		sequenceInfo:    sequence.NewSequenceInfo(),
		ackQueue:        structures.NewRingBuffer[unackedPacket](),
		incomingPackets: make(chan *Packet, config.MaxIncomingPackets),
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
	// only accept packet if sequence number is greater or equal to this connections incoming sequence number
	if !c.sequenceInfo.Accept(p.GetSn()) {
		return
	}

	log.Printf("Received payload: '%s' (%d bytes)", string(p.Payload), p.PayloadLength)
	log.Printf("New incoming sequence number = %d", c.sequenceInfo.NextIn.Value)
}
