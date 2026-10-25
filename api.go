package rapide

import "context"

// Listen returns a Listener listening at the given local address.
func Listen(localAddr string) (*Listener, error) {
	return newListener(localAddr)
}

// Dial returns a client connected to the given remote address.
func Dial(ctx context.Context, remoteAddr string) (Connection, error) {
	return newClient(ctx, remoteAddr)
}
