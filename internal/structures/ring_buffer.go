package structures

import "errors"

var ringBufferOutOfBounds error = errors.New("out of bounds access on RingBuffer")
var ringBufferPopFromEmpty error = errors.New("cannot pop from empty RingBuffer")

// An RingBuffer/ArrayDeque from OpenDataStructures
type RingBuffer[T any] struct {
	// start of the ring buffer
	start int

	// number of elements in the buffer
	size int

	// backing array / buffer for the elements
	buffer []T
}

// NewRingBuffer creates a new RingBuffer with an initial size of 0.
func NewRingBuffer[T any]() *RingBuffer[T] {
	return &RingBuffer[T]{start: 0, size: 0, buffer: nil}
}

// AddFirst adds 'x' to the start of the RingBuffer.
func (r *RingBuffer[T]) AddFirst(x T) {
	_ = r.Add(0, x)
}

// AddLast adds 'x' to the end of the RingBuffer.
func (r *RingBuffer[T]) AddLast(x T) {
	_ = r.Add(r.size, x)
}

// PopFirst removes and returns the element at the start of the RingBuffer.
func (r *RingBuffer[T]) PopFirst() (*T, error) {
	if r.size == 0 {
		return nil, ringBufferPopFromEmpty
	}

	return r.Remove(0)
}

// PopLast removes and returns the element at the end of the RingBuffer.
func (r *RingBuffer[T]) PopLast() (*T, error) {
	if r.size == 0 {
		return nil, ringBufferPopFromEmpty
	}

	return r.Remove(r.size - 1)
}

// Get returns the element at index 'i' in the RingBuffer.
func (r *RingBuffer[T]) Get(i int) (*T, error) {
	if i < 0 || i >= r.size {
		return nil, ringBufferOutOfBounds
	}

	return &r.buffer[r.bufIdx(i)], nil
}

// Set sets the element at index 'i' to 'x'. Returns the element at index 'i' previously.
func (r *RingBuffer[T]) Set(i int, x T) (*T, error) {
	if i < 0 || i >= r.size {
		return nil, ringBufferOutOfBounds
	}

	y := r.buffer[r.bufIdx(i)]
	r.buffer[r.bufIdx(i)] = x

	return &y, nil
}

// Add adds 'x' to index 'i' of the RingBuffer.
func (r *RingBuffer[T]) Add(i int, x T) error {
	if i < 0 || i > r.size {
		return ringBufferOutOfBounds
	}

	if r.size == len(r.buffer) {
		r.resize()
	}

	if i < r.size/2 {
		// shift elements from [0, i] to the left, and move the RingBuffer start down
		if r.start == 0 {
			r.start = len(r.buffer) - 1
		} else {
			r.start = r.start - 1
		}

		for k := range i {
			r.buffer[r.bufIdx(k)] = r.buffer[r.bufIdx(k+1)]
		}
	} else {
		// shift elements from [i, n-1] to the right
		for k := r.size; k > i; k-- {
			r.buffer[r.bufIdx(k)] = r.buffer[r.bufIdx(k-1)]
		}
	}

	r.buffer[r.bufIdx(i)] = x
	r.size++

	return nil
}

// Remove removes the element at index 'i' and returns it.
func (r *RingBuffer[T]) Remove(i int) (*T, error) {
	if i < 0 || i >= r.size {
		return nil, ringBufferOutOfBounds
	}

	y := r.buffer[r.bufIdx(i)]

	if i < r.size/2 {
		// shift elements from [0, i-1] to the right, and move the RingBuffer start up
		for k := i; k > 0; k-- {
			r.buffer[r.bufIdx(k)] = r.buffer[r.bufIdx(k-1)]
		}

		r.start = (r.start + 1) % len(r.buffer)
	} else {
		// shift elements from [i+1, n-1] to the left
		for k := i; k < r.size-1; k++ {
			r.buffer[r.bufIdx(k)] = r.buffer[r.bufIdx(k+1)]
		}
	}

	r.size--

	if len(r.buffer) >= 3*r.size {
		r.resize()
	}

	return &y, nil
}

// Size returns the number of elements in the RingBuffer.
func (r *RingBuffer[T]) Size() int {
	return r.size
}

// resize resizes the RingBuffer's 'buffer' to max(1, 2*r.size) elements - it copies all elements from the old buffer to the new buffer.
func (r *RingBuffer[T]) resize() {
	newBuffer := make([]T, max(1, 2*r.size))

	for i := range r.size {
		newBuffer[i] = r.buffer[r.bufIdx(i)]
	}

	r.buffer = newBuffer
	r.start = 0
}

// Returns the physical index of 'i' in the RingBuffer.
func (r *RingBuffer[T]) bufIdx(i int) int {
	return (r.start + i) % len(r.buffer)
}
