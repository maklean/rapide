package rapide

import (
	"net"

	"github.com/maklean/rapide/internal/sequence"
)

// Dial creates a connection to the given address.
func Dial(address string) (*connection, error) {
	addr, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		return nil, err
	}

	socket, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		return nil, err
	}

	conn := &connection{socket: socket, peerAddr: addr, sequenceInfo: sequence.NewSequenceInfo()}

	go conn.readLoop()

	return conn, nil
}
