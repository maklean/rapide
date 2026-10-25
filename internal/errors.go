package internal

import "errors"

var ErrListenerBacklogFull error = errors.New("listener's backlog is full, cannot accept new client")
var ErrNoAvailableWriter error = errors.New("no available writer for udp connection")
var ErrInvalidConnectionID error = errors.New("invalid connection ID")
var ErrHandshakeTimeout error = errors.New("handshake request timed out")
var ErrCloseRequest error = errors.New("connection close request sent")
var ErrListenerConnectionClosed error = errors.New("listener connection closed")
var ErrClientConnectionClosed error = errors.New("client connection closed")
