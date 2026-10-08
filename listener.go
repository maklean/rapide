package rapide

import (
	"log"
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
	peers map[string]*connection
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

	return &Listener{socket: socket, peers: make(map[string]*connection)}, nil
}

// Listen creates a socket that starts listening at the given address.
func Listen(address string) (*Listener, error) {
	listener, err := newListener(address)
	if err != nil {
		return nil, err
	}

	go listener.readLoop()

	return listener, nil
}

// readLoop is the Listener's read loop - it accepts datagrams, maps them to an existing connection (or creates one), and sends a packet into the connection's incoming packet channel.
func (l *Listener) readLoop() {
	buf := make([]byte, config.MaxPayloadSize)

	for {
		n, addr, err := l.socket.ReadFromUDP(buf)
		if err != nil {
			log.Fatalln("Failed to read data from incoming datagram:", err)
		}

		packet, err := FromBytes(buf[:n])
		if err != nil {
			log.Println("Failed to decode payload from incoming datagram:", err)
			continue
		}

		// TODO: implement ConnectionID system later, currently this is vulnerable to NAT mapping changes
		l.peersMu.Lock()

		key := addr.String()
		conn, exists := l.peers[key]

		// add to peers map if it's a new peer
		if !exists {
			conn = newConnection(&writer.ListenerWriter{Socket: l.socket, PeerAddr: addr})
			l.peers[key] = conn
		}

		conn.deliver(packet)

		l.peersMu.Unlock()
	}
}
