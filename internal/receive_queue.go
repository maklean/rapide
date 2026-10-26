package internal

import (
	"context"
	"sync"
)

type ReceiveQueue struct {
	mu     sync.Mutex
	size   int
	start  int
	notify chan struct{}
	buf    [][]byte
}

// NewReceiveQueue initializes a new ReceiveQueue with the given capacity.
func NewReceiveQueue(capacity int) *ReceiveQueue {
	if capacity <= 0 {
		panic("invalid capacity <= 0 passed to NewReceiveQueue")
	}

	return &ReceiveQueue{
		start:  0,
		size:   0,
		notify: make(chan struct{}, 1),
		buf:    make([][]byte, capacity),
	}
}

// Push attempts to push the given data into the ReceiveQueue. Returns whether there was enough space to do so.
func (r *ReceiveQueue) Push(data []byte) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.size == len(r.buf) {
		return false
	}

	r.buf[r.i(r.size)] = data
	r.size++

	r.wake()

	return true
}

// Pop returns the head of the ReceiveQueue. If no data is immediately available, Pop blocks until it can return something.
func (r *ReceiveQueue) Pop(ctx context.Context) ([]byte, error) {
	for {
		r.mu.Lock()

		if r.size > 0 {
			data := r.buf[r.start]

			r.buf[r.start] = nil
			r.start = r.i(1)

			r.size--

			// there could be other goroutines waiting on data, re-send notify signal
			if r.size > 0 {
				r.wake()
			}

			r.mu.Unlock()

			return data, nil
		}

		r.mu.Unlock()

		select {
		case <-ctx.Done():
			return nil, context.Cause(ctx)
		case <-r.notify:
		}
	}
}

// Free returns the number of free slots in the ReceiveQueue.
func (r *ReceiveQueue) Free() int {
	return len(r.buf) - r.size
}

// Capacity returns the capacity of the ReceiveQueue.
func (r *ReceiveQueue) Capacity() int {
	return len(r.buf)
}

// i returns the proper index in the ReceiveQueue using modular arithmetic.
func (r *ReceiveQueue) i(index int) int {
	return (r.start + index) % len(r.buf)
}

// wake attempts to sends a signal into the ReceiveQueue's notify channel.
func (r *ReceiveQueue) wake() {
	select {
	case r.notify <- struct{}{}:
	default:
	}
}
