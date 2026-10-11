package rapide

import (
	"errors"
	"net"
	"sync"

	"github.com/maklean/rapide/internal/config"
	"github.com/maklean/rapide/internal/sequence"
	"github.com/maklean/rapide/internal/structures"
	"github.com/maklean/rapide/internal/writer"
)

type connection struct {
	// identifier for this connection
	connectionID uint32

	// socket for this connection
	socket *net.UDPConn

	// packet writer for this connection
	writer writer.PacketWriter

	// next incoming sequence number
	nextIn sequence.SequenceNumber

	// next outgoing sequence number
	nextOut sequence.SequenceNumber

	// queue of packets waiting to be acknowledged
	ackQueue *structures.RingBuffer[unackedPacket]

	// mutex for the ackQueue
	ackQueueMu sync.Mutex

	// incoming packets channel for connection
	incomingPackets chan *Packet

	// handshake channel
	established chan struct{}

	// needed for a thread-safe handshake establishment
	establishedOnce sync.Once
}

// Send sends a byte payload to the peer address of this connection
func (c *connection) Send(payload []byte) error {
	if !c.isEstablished() {
		return errors.New("connection not established")
	}

	packet := Packet{
		ConnectionID:      c.connectionID,
		RawSequenceNumber: c.nextOut.Value,
		PayloadLength:     uint16(len(payload)),
		Payload:           payload,
	}

	data, err := packet.ToBytes()
	if err != nil {
		return err
	}

	err = c.writer.Write(data)

	if err == nil {
		c.nextOut = c.nextOut.Next()
	}

	return err
}

// newConnection creates a new connection struct instance who's connected to the given peer address.
func newConnection(socket *net.UDPConn, writer writer.PacketWriter) *connection {
	conn := &connection{
		socket:          socket,
		writer:          writer,
		ackQueue:        structures.NewRingBuffer[unackedPacket](),
		incomingPackets: make(chan *Packet, config.MaxIncomingPackets),
		established:     make(chan struct{}),
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
	if p.IsHandshakeServer {
		// set connection ID if this is a handshake packet, and closed established channel.
		c.establishedOnce.Do(func() {
			c.connectionID = p.ConnectionID
			close(c.established)
		})

		return
	}

	// only accept packet if sequence number is greater or equal to this connections incoming sequence number
	if c.nextIn.Gt(p.GetSn()) {
		return
	}
}

// isEstablished returns whether a connection with a peer was successfully established.
func (c *connection) isEstablished() bool {
	return c.connectionID != 0
}
