package rapide

import (
	"log"
	"net"
)

type Listener struct {
	// socket for the listener
	socket *net.UDPConn

	// addr => connection instances that are connected to this Listener
	peers map[string]*connection
}

// Listen creates a socket that starts listening at the given address.
func Listen(address string) (*Listener, error) {
	addr, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		return nil, err
	}

	socket, err := net.ListenUDP("udp", addr)
	if err != nil {
		return nil, err
	}

	listener := &Listener{socket: socket, peers: make(map[string]*connection)}

	go listener.readLoop()

	return listener, nil
}

// readLoop starts the read loop for the current Listener
func (l *Listener) readLoop() {
	buf := make([]byte, 1500)

	for {
		n, addr, err := l.socket.ReadFromUDP(buf)
		if err != nil {
			return
		}

		key := addr.String()
		conn, exists := l.peers[key]

		// create new connection entry in peers map if it doesn't exist yet
		if !exists {
			log.Printf("Listener: accepted new connection '%s'...", key)
			conn = &connection{socket: l.socket, peerAddr: addr}
			l.peers[key] = conn
		}

		data := buf[:n]
		strData := string(data)
		log.Printf("Received '%s' from '%s'", strData, key)
	}
}
