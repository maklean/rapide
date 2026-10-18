package rapide

import (
	"errors"
	"log"
	"net"
	"sync"
	"time"

	"github.com/maklean/rapide/internal/config"
	"github.com/maklean/rapide/internal/sequence"
	"github.com/maklean/rapide/internal/writer"
)

type connection struct {
	// identifier for this connection
	connectionID uint32

	// socket for this connection
	socket *net.UDPConn

	// packet writer for this connection
	writer writer.PacketWriter

	// reliable sequence numbers
	reliableSp sequence.SequencePack

	// unreliable sequence numbers
	unreliableSp sequence.SequencePack

	// map of reliable packets waiting to be acknowledged
	unacked map[uint16]*unackedPacket

	// map of reliable packets that have arrived
	pending map[uint16]*Packet

	// incoming packets channel for connection
	incoming chan *Packet

	// outgoing reliable payloads channel for connection
	relOutgoing chan []byte

	// outgoing unreliable payloads channel for connection
	unrelOutgoing chan []byte

	// signal for closing connection
	closed chan struct{}

	// needed for a thread-safe connection termination
	closedOnce sync.Once

	// closure called as the connection's about to close (listener/server = rmv. from peers map, client = close socket)
	onClose func()

	// signal for when we're finished closing/running the connection's loop
	finished chan struct{}
}

// Send sends a byte payload to the peer address of this connection
func (c *connection) Send(payload []byte, reliable bool) error {
	if c.isClosed() {
		return errors.New("connection is closed")
	}

	// sender could reuse payload later
	cpy := make([]byte, len(payload))
	copy(cpy, payload)

	if reliable {
		select {
		case c.relOutgoing <- cpy:
		default:
			return errors.New("outgoing queue full")
		}
	} else {
		select {
		case c.unrelOutgoing <- cpy:
		default:
		}
	}

	return nil
}

// Close closes the connection with the peer.
func (c *connection) Close() error {
	if c.isClosed() {
		return errors.New("connection is closed")
	}

	c.closedOnce.Do(func() {
		close(c.closed)
	})

	<-c.finished

	return nil
}

// newConnection creates a new connection struct instance whose connected to the given peer address.
func newConnection(connectionID uint32, socket *net.UDPConn, writer writer.PacketWriter, onClose func()) *connection {
	conn := &connection{
		connectionID:  connectionID,
		socket:        socket,
		writer:        writer,
		onClose:       onClose,
		reliableSp:    sequence.NewSequencePack(),
		unreliableSp:  sequence.NewSequencePack(),
		unacked:       make(map[uint16]*unackedPacket),
		pending:       make(map[uint16]*Packet),
		incoming:      make(chan *Packet, config.MaxIncomingPackets),
		relOutgoing:   make(chan []byte, config.MaxOutgoingPackets),
		unrelOutgoing: make(chan []byte, config.MaxIncomingPackets),
		closed:        make(chan struct{}),
		finished:      make(chan struct{}),
	}

	// connection loop - handles incoming packets for connection
	go conn.run()

	return conn
}

// deliver adds the given packet to the connections incoming packets channel.
func (c *connection) deliver(p *Packet) {
	select {
	case c.incoming <- p:
	default:
	}
}

// run is the client's read loop
func (c *connection) run() {
	defer close(c.finished)

	ticker := time.NewTicker(time.Duration(config.ConnectionTick) * time.Millisecond)

	for {
		select {
		case packet := <-c.incoming:
			if packet.IsReliable || packet.IsAck {
				c.handleReliableIncoming(packet)
			} else {
				c.handleUnreliableIncoming(packet)
			}
		case payload := <-c.relOutgoing:
			c.handleOutgoing(payload, true)
		case payload := <-c.unrelOutgoing:
			c.handleOutgoing(payload, false)
		case t := <-ticker.C:
			c.handleTick(t)
		case <-c.closed:
			// end ticker, flush (only) reliable outgoing channel, run close closure, send finished signal by exiting
			ticker.Stop()

			for {
				select {
				case payload := <-c.relOutgoing:
					c.handleOutgoing(payload, true)
				default:
					if c.onClose != nil {
						c.onClose()
					}

					return
				}
			}
		}
	}
}

// handleIncoming handles the given packet from the peer.
func (c *connection) handleReliableIncoming(p *Packet) {
	if p.IsAck {
		// NOTE: this should handle duplicate ACKs because it no-ops on non-existent keys
		log.Printf("server acknowledged %d, deleting...", p.AckSequenceNumber)
		delete(c.unacked, p.AckSequenceNumber)
		return
	}

	sn := p.GetSn()
	dist := sn.Distance(c.reliableSp.NextIn)

	if dist < 0 {
		// lost ACK or duplicate packet, either way resend ACK
		c.sendAck(sn)
		return
	}

	if dist >= int16(config.ReceiveWindowLength) {
		// only accept packets in our receive window
		return
	}

	c.sendAck(sn)

	c.pending[sn.Value] = p
	log.Printf("pending on %d", sn.Value)

	// reorder packets that came in
	for {
		pending, exists := c.pending[c.reliableSp.NextIn.Value]

		// force correct order
		if !exists {
			log.Printf("%d hasn't arrived yet, break...", c.reliableSp.NextIn.Value)
			break
		}

		delete(c.pending, c.reliableSp.NextIn.Value)
		c.reliableSp.NextIn = c.reliableSp.NextIn.Next()
		log.Printf("Got: %s", string(pending.Payload))
	}
}

func (c *connection) handleUnreliableIncoming(p *Packet) {
	sn := p.GetSn()

	if c.unreliableSp.NextIn.Gt(sn) {
		// reject if the next incoming sequence number is greater, reject
		return
	}

	c.unreliableSp.NextIn = sn.Next()

	log.Printf("got unreliable (sn = %d): '%s'", p.RawSequenceNumber, string(p.Payload))
}

// sendAck sends an acknowledgement packet for the given sequence number to the peer
func (c *connection) sendAck(sn sequence.SequenceNumber) {
	ackPacket, err := (&Packet{ConnectionID: c.connectionID, IsAck: true, AckSequenceNumber: sn.Value}).ToBytes()
	if err != nil {
		return
	}

	err = c.writer.Write(ackPacket)
	if err != nil {
		return
	}

	log.Println("Sent ACK packet to peer.")
}

// handleOutgoing handles sending the given payload to the peer.
func (c *connection) handleOutgoing(payload []byte, reliable bool) {
	var sn uint16

	if reliable {
		sn = c.reliableSp.NextOut.Value
	} else {
		sn = c.unreliableSp.NextOut.Value
	}

	packet := Packet{
		ConnectionID:      c.connectionID,
		RawSequenceNumber: sn,
		PayloadLength:     uint16(len(payload)),
		Payload:           payload,
		IsReliable:        reliable,
	}

	data, err := packet.ToBytes()
	if err != nil {
		return
	}

	if err := c.writer.Write(data); err != nil {
		return
	}

	if reliable {
		c.unacked[c.reliableSp.NextOut.Value] = &unackedPacket{packet: data, sentAt: time.Now(), retries: 0}
		c.reliableSp.NextOut = c.reliableSp.NextOut.Next()
	} else {
		c.unreliableSp.NextOut = c.unreliableSp.NextOut.Next()
	}
}

// handleTick handles a tick at the given time for the connection.
func (c *connection) handleTick(t time.Time) {
	// check for retransmission of unacked packets
	for sn, up := range c.unacked {
		if t.Sub(up.sentAt).Seconds() < float64(config.ReliablePacketTimeout) {
			continue
		}

		if up.retries >= uint8(config.MaxRetransmissionRetries) {
			// connection is considered dead once we've passed the max number of retries
			log.Printf("MaxRetransmissionsRetries passed for 'sn = %d', closing connection with peer...", sn)

			c.closedOnce.Do(func() {
				close(c.closed)
			})
			return
		}

		// retransmit packet
		if err := c.writer.Write(up.packet); err == nil {
			log.Printf("retransmitted packet (sn = %d) because it went unacknowledged.", sn)
			up.sentAt = time.Now()
			up.retries += 1
		}
	}
}

// isClosed returns whether a connection with a peer was terminated.
func (c *connection) isClosed() bool {
	select {
	case <-c.closed:
		return true
	default:
		return false
	}
}
