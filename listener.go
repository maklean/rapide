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

		// we're gonna assume a connectionID of 0 means they want to perform a handshake.
		if packet.ConnectionID == 0 {
			if !packet.IsConnect {
				log.Println("[ignore] received connection ID of 0 on a non-handshake packet")
				continue
			}

			// create new peer entry on handshake
			conn := newConnection(l.socket, writer.CreateListenerWriter(l.socket, addr))
			conn.connectionID = l.generateConnectionID()
			close(conn.established) // not necessary on the server side.

			l.peers[conn.connectionID] = conn

			handshakePacket := Packet{ConnectionID: conn.connectionID, IsHandshake: true}
			data, err := handshakePacket.ToBytes()
			if err != nil {
				continue
			}

			err = conn.writer.Write(data)
			if err != nil {
				continue
			}

			log.Printf("Sent handshake to client with connectionID = %d", conn.connectionID)

			l.peersMu.Unlock()

			continue
		}

		conn, exists := l.peers[packet.ConnectionID]
		if !exists {
			// unknown connection ID that hasn't established a connection with the server
			continue
		}

		log.Printf("Handling packet from connection with ID = %d", conn.connectionID)

		conn.deliver(packet)

		l.peersMu.Unlock()
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
