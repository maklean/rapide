package rapide

import (
	"errors"
	"log"
	"math"
	"math/rand/v2"
	"net"
	"sync"

	"github.com/maklean/rapide/internal/config"
	"github.com/maklean/rapide/internal/writer"
)

type Listener struct {
	// socket for the listener
	socket *net.UDPConn

	// mutex for the peers map
	peersMu sync.Mutex

	// addr => connection instances that are connected to this Listener
	peers map[uint32]*connection
}

// newListener creates a new Listener struct instance.
func newListener(address string) (*Listener, error) {
	addr, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		return nil, err
	}

	socket, err := net.ListenUDP("udp", addr)
	if err != nil {
		return nil, err
	}

	return &Listener{socket: socket, peers: make(map[uint32]*connection)}, nil
}

// Listen creates a socket that starts listening at the given address.
func Listen(address string) (*Listener, error) {
	listener, err := newListener(address)
	if err != nil {
		return nil, err
	}

	go listener.listenerReadLoop()

	return listener, nil
}

// listenerReadLoop is the Listener's read loop - it accepts datagrams, maps them to an existing connection (or creates one), and sends a packet into the connection's incoming packet channel.
func (l *Listener) listenerReadLoop() {
	buf := make([]byte, config.MaxPayloadSize)

	for {
		n, addr, err := l.socket.ReadFromUDP(buf)
		if err != nil {
			// if the Listener socket is closed, exit.
			if errors.Is(err, net.ErrClosed) {
				return
			}

			log.Println("Failed to read data from incoming datagram:", err)
			continue
		}

		packet, err := FromBytes(buf[:n])
		if err != nil {
			log.Println("Failed to decode payload from incoming datagram:", err)
			continue
		}

		if packet.ConnectionID == 0 {
			if !packet.IsHandshakeClient {
				continue
			}

			// create new peer entry on handshake
			if err := l.performHandshake(addr); err != nil {
				log.Printf("Failed to perform handshake with client: %v", err)
			}

			continue
		}

		l.peersMu.Lock()
		conn, exists := l.peers[packet.ConnectionID]
		l.peersMu.Unlock()

		if !exists {
			// unknown connection ID that hasn't established a connection with the server
			continue
		}

		conn.deliver(packet)
	}
}

/*
generateConnectionID returns an unused connectionID for a new peer connection.
The generated connectionID is in the range: [1, math.MaxUint32]
*/
func (l *Listener) generateConnectionID() uint32 {
	connectionID := 1 + uint32(rand.IntN(math.MaxUint32))

	_, exists := l.peers[connectionID]

	for exists {
		// generate new connectionID until we proc something that isn't used
		connectionID = 1 + uint32(rand.IntN(math.MaxUint32))
		_, exists = l.peers[connectionID]
	}

	return connectionID
}

// performHandshake creates a new peer entry for the given connection, and sends a handshake packet to them.
func (l *Listener) performHandshake(addr *net.UDPAddr) error {
	conn := newConnection(l.socket, writer.CreateListenerWriter(l.socket, addr), nil)
	close(conn.established) // not necessary on the server side.

	l.peersMu.Lock()
	conn.connectionID = l.generateConnectionID()
	l.peers[conn.connectionID] = conn
	l.peersMu.Unlock()

	// removes the peer from the peers map
	removePeer := func() {
		l.peersMu.Lock()
		delete(l.peers, conn.connectionID)
		l.peersMu.Unlock()
	}

	conn.onClose = func() {
		log.Println("removing peer...")
		l.peersMu.Lock()
		delete(l.peers, conn.connectionID)
		l.peersMu.Unlock()
	}

	// send raw handshake packet to client
	handshakePacket := Packet{ConnectionID: conn.connectionID, IsHandshakeServer: true}
	data, err := handshakePacket.ToBytes()
	if err != nil {
		removePeer()
		return err
	}

	if err := conn.writer.Write(data); err != nil {
		removePeer()
		return err
	}

	return nil
}
