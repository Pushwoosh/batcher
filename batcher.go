package batcher

import (
	"errors"
	"sync"
	"time"
)

// Batcher is a structure that collects items and flushes buffer
// either when the buffer fills up or when timeout is reached.
// See examples/batcher/batcher.go for usage example.
//
// The consumer must read C() until it is closed: output holds only 10 batches,
// and once it is full Add, AddE and Close block until a batch is received.
type Batcher[T any] struct {
	ticker   *time.Ticker
	duration time.Duration
	capacity int

	buffer []T
	output chan []T
	done   chan struct{}

	mu         sync.Mutex
	closed     bool
	timerReset bool
}

// New creates a new instance of Batcher.
func New[T any](d time.Duration, capacity int) *Batcher[T] {
	if capacity <= 0 {
		panic("capacity must be greater than 0")
	}

	obj := &Batcher[T]{}

	obj.ticker = time.NewTicker(d)
	obj.duration = d
	obj.capacity = capacity

	obj.buffer = make([]T, 0, obj.capacity)
	obj.output = make(chan []T, 10) // number of batches to keep
	obj.done = make(chan struct{})

	go func() {
		for {
			// ticker.Stop does not close ticker.C, so Close signals exit via done
			select {
			case <-obj.done:
				return
			case <-obj.ticker.C:
			}

			obj.mu.Lock()
			// the object may be closed while we were waiting for the lock
			// it is safe to exit, the last batch has been already processed
			if obj.closed {
				obj.mu.Unlock()
				return
			}

			// Skip this tick if timer was just reset due to capacity flush
			if obj.timerReset {
				obj.timerReset = false
				obj.mu.Unlock()
				continue
			}

			obj.makeBatch()
			obj.mu.Unlock()
		}
	}()

	return obj
}

// Add adds an item to the batcher.
// It blocks while the output channel is full, see Batcher.
func (b *Batcher[T]) Add(item T) {
	err := b.AddE(item)
	if err != nil {
		panic(err)
	}
}

// AddE adds an item to the batcher.
// It returns an error if the batcher is closed.
func (b *Batcher[T]) AddE(item T) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return errors.New("batcher is closed")
	}

	b.buffer = append(b.buffer, item)
	if len(b.buffer) >= b.capacity {
		b.makeBatch()
		// Reset the timer since we just flushed due to capacity
		b.timerReset = true
		b.ticker.Reset(b.duration)
	}

	return nil
}

// C returns a channel that will receive batches.
// It is closed by Close and must be drained until then.
func (b *Batcher[T]) C() <-chan []T {
	return b.output
}

// Close flushes the remaining items and closes C().
// It blocks if C() is full and nobody reads it.
func (b *Batcher[T]) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return
	}

	b.ticker.Stop()
	b.closed = true
	close(b.done)

	b.makeBatch()
	close(b.output)
}

func (b *Batcher[T]) makeBatch() {
	if len(b.buffer) == 0 {
		return
	}
	b.output <- b.buffer
	b.buffer = make([]T, 0, b.capacity)
}
