package rapide

import (
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
			log.Println("Failed to read data from incoming datagram:", err)
			continue
		}

		packet, err := FromBytes(buf[:n])
		if err != nil {
			log.Println("Failed to decode payload from incoming datagram:", err)
			continue
		}

		// TODO: implement ConnectionID system later, currently this is vulnerable to NAT mapping changes
		l.peersMu.Lock()

		if packet.ConnectionID == 0 {
			if !packet.IsConnect {
				log.Println("[ignore] received connection ID of 0 on a non-handshake packet")
				continue
			}

			// create new peer entry on handshake
			conn := newConnection(l.socket, writer.CreateListenerWriter(l.socket, addr))
			packet.ConnectionID = l.generateConnectionID()
			l.peers[uint32(packet.ConnectionID)] = conn
		}

		conn, exists := l.peers[key]

		// add to peers map if it's a new peer
		if !exists {

		}

		conn.deliver(packet)

		l.peersMu.Unlock()
	}
}

// generateConnectionID returns a new (unused) connectionID for a new peer connection
func (l *Listener) generateConnectionID() uint32 {
	connectionID := uint32(rand.IntN(math.MaxUint32))

	_, exists := l.peers[connectionID]

	for !exists {
		// generate new connectionID until we proc something that isn't used
		connectionID = uint32(rand.IntN(math.MaxUint32))
		_, exists = l.peers[connectionID]
	}

	return connectionID
}
