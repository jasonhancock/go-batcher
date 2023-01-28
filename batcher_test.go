package batcher

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBatcher(t *testing.T) {
	maxWait := 1 * time.Second

	t.Run("basic operation - fewer than max items, maxWait should force them to be processed", func(t *testing.T) {
		b := NewBatchMaxItems[int](5)

		var lock sync.Mutex
		var itemsProcessed []int
		fn := func(items []int) {
			lock.Lock()
			itemsProcessed = append(itemsProcessed, items...)
			lock.Unlock()
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var wg sync.WaitGroup
		batcher := NewBatcher[int](ctx, &wg, maxWait, b, fn)

		require.NoError(t, batcher.AddItem(1))
		time.Sleep(2 * maxWait)

		// check that the item has been processed
		lock.Lock()
		require.Len(t, itemsProcessed, 1)
		require.Equal(t, 1, itemsProcessed[0])
		lock.Unlock()

		cancel()
		wg.Wait()
	})

	t.Run("basic operation - added more than maxItems", func(t *testing.T) {
		b := NewBatchMaxItems[int](5)

		var lock sync.Mutex
		var itemsProcessed []int
		processedCalls := 0
		fn := func(items []int) {
			lock.Lock()
			processedCalls++
			itemsProcessed = append(itemsProcessed, items...)

			switch processedCalls {
			case 1:
				require.Len(t, items, 5)
			case 2:
				require.Len(t, items, 3)
			default:
				t.Fatal("shouldn't be here")
			}

			lock.Unlock()
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var wg sync.WaitGroup
		batcher := NewBatcher[int](ctx, &wg, maxWait, b, fn)

		for i := 0; i < 8; i++ {
			require.NoError(t, batcher.AddItem(i))
		}

		// give it time to propagate through the channel and get processed
		time.Sleep(100 * time.Millisecond)

		// verify the first 5 items got processed
		lock.Lock()
		require.Len(t, itemsProcessed, 5)
		require.Equal(t, processedCalls, 1)
		lock.Unlock()

		time.Sleep(2 * maxWait)

		// check that the second batch hit maxWait and has been processed
		lock.Lock()
		require.Len(t, itemsProcessed, 8)
		require.Equal(t, 0, itemsProcessed[0])
		require.Equal(t, processedCalls, 2)
		lock.Unlock()

		cancel()
		wg.Wait()
	})

	t.Run("basic operation - verify can't add more items after context closed", func(t *testing.T) {
		b := NewBatchMaxItems[int](5)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var wg sync.WaitGroup
		batcher := NewBatcher[int](ctx, &wg, maxWait, b, nil)

		cancel()
		wg.Wait()

		err := batcher.AddItem(1)
		require.Error(t, err)
		require.Equal(t, errNewItems, err)
	})

	t.Run("basic operation - verify metrics", func(t *testing.T) {
		t.Run("queue depth", func(t *testing.T) {
			b := NewBatchMaxItems[int](1)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var wg sync.WaitGroup

			metricLength := &gauge{}
			metricCap := &gauge{}

			opts := []Option{
				WithMetricQueueDepth(metricLength, metricCap, 1*time.Millisecond),
				WithQueueDepth(10),
			}

			fn := func(items []int) {
				time.Sleep(10 * time.Second)
			}
			batcher := NewBatcher[int](ctx, &wg, 1*time.Minute, b, fn, opts...)

			require.NoError(t, batcher.AddItem(1))
			require.NoError(t, batcher.AddItem(2))
			time.Sleep(300 * time.Millisecond)

			require.Equal(t, 1.0, metricLength.Get())
			require.Equal(t, 10.0, metricCap.Get())

			cancel()
		})

		t.Run("processed counters", func(t *testing.T) {
			b := NewBatchMaxItems[int](5)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var wg sync.WaitGroup

			metricBatches := &counter{}
			metricItems := &counter{}

			opts := []Option{
				WithProcessedCounters(metricBatches, metricItems),
			}

			fn := func(items []int) {}
			batcher := NewBatcher[int](ctx, &wg, maxWait, b, fn, opts...)

			require.NoError(t, batcher.AddItem(1))
			require.NoError(t, batcher.AddItem(2))

			time.Sleep(2 * maxWait)

			require.Equal(t, 1.0, metricBatches.Get())
			require.Equal(t, 2.0, metricItems.Get())

			cancel()
		})

	})
}

type counter struct {
	count float64
	lock  sync.Mutex
}

func (c *counter) Inc() {
	c.Add(1)
}

func (c *counter) Add(val float64) {
	c.lock.Lock()
	c.count += val
	c.lock.Unlock()
}

func (c *counter) Get() (val float64) {
	c.lock.Lock()
	val = c.count
	c.lock.Unlock()
	return
}

type gauge struct {
	value float64
	lock  sync.Mutex
}

func (g *gauge) Set(val float64) {
	g.lock.Lock()
	g.value = val
	g.lock.Unlock()
}

func (g *gauge) Get() (val float64) {
	g.lock.Lock()
	val = g.value
	g.lock.Unlock()
	return
}
