package batcher

import (
	"context"
	"errors"
	"sync"
	"time"
)

// Counter is an increasing metrics value.
type Counter interface {
	Inc()
	Add(float64)
}

// Gauge is a value that varies over time.
type Gauge interface {
	// Set sets the Gauge to an arbitrary value.
	Set(float64)
}

// Batcher will batch up a workload and process it in chunks. Tries to guarantee
// that any item added to the batcher will wait at most a set amount of time.
type Batcher[T any] struct {
	maxWait time.Duration
	batch   Batch[T]
	queue   chan T

	lock sync.RWMutex
	done bool

	// This wait group ensures that after the context is closed, that all items that
	// have been added are processed.
	waitGroup *sync.WaitGroup

	handlerFn func(items []T)

	counterBatchesProcessed Counter
	counterItemsProcessed   Counter
}

// NewBatcher initializes a Batcher. maxWait is the maximimum amount of time you
// want any single item to stay in queue. handler will get called with a slice of
// items when it's time to process the batch.
func NewBatcher[T any](ctx context.Context, wg *sync.WaitGroup, maxWait time.Duration, batch Batch[T], handler func([]T), opts ...Option) *Batcher[T] {
	opt := options{
		queueDepth: 10000,
	}
	for _, o := range opts {
		o(&opt)
	}

	b := Batcher[T]{
		batch:                   batch,
		maxWait:                 maxWait,
		done:                    false,
		queue:                   make(chan T, opt.queueDepth),
		waitGroup:               wg,
		handlerFn:               handler,
		counterBatchesProcessed: opt.metricBatchesProcessed,
		counterItemsProcessed:   opt.metricItemsProcessed,
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		b.run(ctx)
	}()

	if opt.metricQueueLength != nil && opt.metricQueueCapacity != nil {
		channelDepth(ctx, b.queue, opt.metricQueueLength, opt.metricQueueCapacity, opt.queueDepthInterval)
	}

	return &b
}

// ErrNewItems is the error returned once the batcher has been shut down.
var ErrNewItems = errors.New("unable to accept new items")

// AddItem inserts an item into the batch. Can block if the internal channel gets
// full. Will return an error if the item cannot be added because the batcher has
// been instructed to shut down.
func (b *Batcher[T]) AddItem(value T) error {
	b.lock.RLock()
	done := b.done
	b.lock.RUnlock()
	if done {
		return ErrNewItems
	}
	b.waitGroup.Add(1)
	b.queue <- value
	return nil
}

func (b *Batcher[T]) run(ctx context.Context) {
	ticker := time.NewTicker(b.maxWait)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			b.lock.Lock()
			b.done = true // stop accepting additional work
			close(b.queue)
			b.lock.Unlock()

			for msg := range b.queue {
				b.waitGroup.Done()
				shouldProcess := b.batch.Add(msg)
				if shouldProcess {
					b.processBatch()
				}
			}

			// catch anything left in the batch
			b.processBatch()

			return
		case <-ticker.C:
			b.processBatch()
		case msg := <-b.queue:
			b.waitGroup.Done()
			shouldProcess := b.batch.Add(msg)
			if shouldProcess {
				b.processBatch()
				ticker.Reset(b.maxWait)
			}
		}
	}
}

func (b *Batcher[T]) processBatch() {
	items := b.batch.Items()
	if len(items) == 0 {
		return
	}

	if b.counterBatchesProcessed != nil {
		b.counterBatchesProcessed.Inc()
	}
	if b.counterItemsProcessed != nil {
		b.counterItemsProcessed.Add(float64(len(items)))
	}

	if b.handlerFn != nil {
		b.handlerFn(items)
	}
}

// Batch types should be able to handle being called on a nil value.
type Batch[T any] interface {
	// Len returns how many items are currently in the batch.
	Len() int
	// Add should add the item to the batch. If the returned bool is true, the batch
	// should be considered full and should be processed.
	Add(T) bool
	// Items should return the list of items in the batch and clear them from the batch.
	Items() []T
}

func channelDepth[T any](ctx context.Context, ch chan T, length, capacity Gauge, interval time.Duration) {
	go func() {
		ticker := time.NewTimer(0)
		for {
			select {
			case <-ctx.Done():
				ticker.Stop()
				return
			case <-ticker.C:
				length.Set(float64(len(ch)))
				capacity.Set(float64(cap(ch)))
				ticker.Reset(interval)
			}
		}
	}()
}
