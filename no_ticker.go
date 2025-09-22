package batcher

import "sync"

// NoTicker collects buffer and flushes only by size or when closed.
type NoTicker[T any] struct {
	capacity int
	buffer   []T
	output   chan []T
	mu       sync.Mutex
	closed   bool
}

// NewNoTicker creates a new batcher with the given batch size.
func NewNoTicker[T any](capacity int) *NoTicker[T] {
	return &NoTicker[T]{
		capacity: capacity,
		buffer:   make([]T, 0, capacity),
		output:   make(chan []T, 10),
	}
}

// Add adds an item to the batcher. If batch is full, it flushes.
func (b *NoTicker[T]) Add(item T) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return
	}
	b.buffer = append(b.buffer, item)
	if len(b.buffer) >= b.capacity {
		b.flush()
	}
}

// flush sends the current batch and resets buffer.
func (b *NoTicker[T]) flush() {
	if len(b.buffer) == 0 {
		return
	}
	batch := make([]T, len(b.buffer))
	copy(batch, b.buffer)
	b.buffer = b.buffer[:0]
	b.output <- batch
}

// Out returns the output channel for flushed batches.
func (b *NoTicker[T]) Out() <-chan []T {
	return b.output
}

// Close flushes any remaining buffer and closes the output channel.
func (b *NoTicker[T]) Close() {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return
	}
	b.closed = true
	b.flush()
	close(b.output)
	b.mu.Unlock()
}
