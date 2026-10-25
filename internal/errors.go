package internal

import "errors"

var ErrListenerBacklogFull error = errors.New("listener's backlog is full, cannot accept new client")
var ErrNoAvailableWriter error = errors.New("no available writer for udp connection")
var ErrInvalidConnectionID error = errors.New("invalid connection ID")
var HandshakeTimeout error = errors.New("handshake request timed out")
