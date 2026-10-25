package rapide

import (
	"fmt"
	"math/rand"
	"net"
	"syscall"

	"github.com/maklean/rapide/internal"
)

type Listener struct {
	// UDP connection for listener
	conn internal.UDPConnection

	// map of connectionID => peer
	peers map[uint32]*connection

	// peers that have established a connection (handshake)
	acceptedPeers chan Connection
}

// newListener creates a new listener listening at the given local address.
func newListener(localAddr string) (*Listener, error) {
	addr, err := net.ResolveUDPAddr("udp", localAddr)
	if err != nil {
		return nil, err
	}

	socket, err := net.ListenUDP("udp", addr)
	if err != nil {
		return nil, err
	}

	listener := &Listener{
		conn:          internal.NewUDPConnection(socket, nil),
		peers:         make(map[uint32]*connection),
		acceptedPeers: make(chan Connection, syscall.SOMAXCONN),
	}

	go listener.conn.Read(listener.handlePacket)

	return listener, nil
}

// handlePacket handles an incoming packet sent by a peer.
func (l *Listener) handlePacket(addr *net.UDPAddr, p *internal.Packet) error {
	if p.IsHandshakeClient {
		return l.handleHandshake(addr)
	}

	_, exists := l.peers[p.ConnectionID]

	if !exists {
		return fmt.Errorf("%w: %d", internal.ErrInvalidConnectionID, p.ConnectionID)
	}

	return nil
}

// handleHandshake handles an incoming handshake request by a client at the given remote address.
func (l *Listener) handleHandshake(addr *net.UDPAddr) error {
	if len(l.acceptedPeers) == cap(l.acceptedPeers) {
		return internal.ErrListenerBacklogFull
	}

	// genConnID generates an unused connection ID.
	genConnID := func() uint32 {
		connId := rand.Uint32()

		_, exists := l.peers[connId]

		for exists {
			connId = rand.Uint32()
			_, exists = l.peers[connId]
		}

		return connId
	}

	socket := l.conn.Socket()
	connId := genConnID()

	conn := newConnection(connId, internal.NewUDPConnection(socket, internal.NewListenerWriter(socket, addr)))
	l.peers[connId] = conn

	// send handshake packet back to client, TODO: close connection on errors
	handshake, err := (&internal.Packet{ConnectionID: connId, IsHandshakeServer: true}).ToBytes()
	if err != nil {
		delete(l.peers, connId)
		return err
	}

	if err := conn.udpConnection.Write(handshake); err != nil {
		delete(l.peers, connId)
		return err
	}

	l.acceptedPeers <- conn

	return nil
}
