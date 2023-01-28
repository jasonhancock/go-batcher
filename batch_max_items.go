package batcher

import "sync"

// BatchMaxItems is a batch that will ask to be processed as soon as there are N
// number of items in the batch.
type BatchMaxItems[T any] struct {
	max   int
	items []T
	lock  sync.Mutex
}

// NewBatchMaxItems initializes a BatchMaxItems for the given type. Once the
// batch contains maxItems, it will ask to be processed.
func NewBatchMaxItems[T any](maxItems int) *BatchMaxItems[T] {
	return &BatchMaxItems[T]{
		max: maxItems,
	}
}

// Len returns the number of items in the batch.
func (b *BatchMaxItems[T]) Len() (l int) {
	b.lock.Lock()
	l = len(b.items)
	b.lock.Unlock()
	return
}

// Add puts another item into the batch. shouldProcess indicates whether or not
// the batch should be processed. For this implementation of a Batch,
// shouldProcess being true means that it contains at least N items, where N was
// configured when NewBatchMaxItems was called.
func (b *BatchMaxItems[T]) Add(value T) (shouldProcess bool) {
	b.lock.Lock()
	b.items = append(b.items, value)
	shouldProcess = len(b.items) >= b.max
	b.lock.Unlock()
	return
}

// Items returns the list of items in the batch and resets the batch back to empty.
func (b *BatchMaxItems[T]) Items() (values []T) {
	// TODO: wonder if we should have this instead only take up to max items. As it sits now, if Add is called and it pushes the batch to max and returns true indicating it should be processed,
	b.lock.Lock()
	n := len(b.items)
	if n > b.max {
		n = b.max
	}
	values, b.items = b.items[:n], b.items[n:]
	b.lock.Unlock()
	return
}
