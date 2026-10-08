package writer

import "net"

// packetWriter is an interface for sending/writing data to packets.
type PacketWriter interface {
	Write(data []byte) error
}

// ListenerWriter is an implementation of packetWriter for socket's from a Listener.
type ListenerWriter struct {
	Socket   *net.UDPConn
	PeerAddr *net.UDPAddr
}

// ClientWriter is an implementation of packetWriter for socket's from a client.
type ClientWriter struct {
	Socket   *net.UDPConn
	PeerAddr *net.UDPAddr
}

func (lw *ListenerWriter) Write(data []byte) error {
	// lw.Socket should be the shared socket between the Listener and its peers (hence, why we need to specify a target address)
	_, err := lw.Socket.WriteToUDP(data, lw.PeerAddr)
	return err
}

func (cw *ClientWriter) Write(data []byte) error {
	// cw.Socket should be a uniquely owned socket connected to a server
	_, err := cw.Socket.Write(data)
	return err
}
