package rapide

import (
	"errors"
	"log"
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

		if packet.IsHandshakeClient {
			log.Printf("Got handshake request from '%s'...", addr.String())

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

// generateConnectionID returns an unused connectionID for a new peer connection.
func (l *Listener) generateConnectionID() uint32 {
	connectionID := rand.Uint32()

	l.peersMu.Lock()
	defer l.peersMu.Unlock()

	_, exists := l.peers[connectionID]

	for exists {
		// generate new connectionID until we proc something that isn't used
		connectionID = rand.Uint32()
		_, exists = l.peers[connectionID]
	}

	return connectionID
}

// performHandshake creates a new peer entry for the given connection, and sends a handshake packet to them.
func (l *Listener) performHandshake(addr *net.UDPAddr) error {
	connID := l.generateConnectionID()

	// removes the peer from the peers map
	removePeer := func() {
		l.peersMu.Lock()
		delete(l.peers, connID)
		l.peersMu.Unlock()
	}

	conn := newConnection(connID, l.socket, writer.CreateListenerWriter(l.socket, addr), removePeer)

	l.peersMu.Lock()
	l.peers[conn.connectionID] = conn
	l.peersMu.Unlock()

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

	log.Printf("Sent handshake response to '%s'... ConnectionID = %d", addr.String(), conn.connectionID)

	return nil
}
