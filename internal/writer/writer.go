package writer

import "net"

// packetWriter is an interface for sending/writing data to packets.
type PacketWriter interface {
	Write(data []byte) error
}

// ListenerWriter is an implementation of packetWriter for socket's from a Listener (shared socket).
type ListenerWriter struct {
	Socket   *net.UDPConn
	PeerAddr *net.UDPAddr
}

// ClientWriter is an implementation of packetWriter for socket's from a client (owned socket).
type ClientWriter struct {
	Socket   *net.UDPConn
	PeerAddr *net.UDPAddr
}

func (lw *ListenerWriter) Write(data []byte) error {
	_, err := lw.Socket.WriteToUDP(data, lw.PeerAddr)
	return err
}

func (cw *ClientWriter) Write(data []byte) error {
	_, err := cw.Socket.Write(data)
	return err
}
