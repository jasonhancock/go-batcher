package batcher

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// AddItem racing shutdown must neither panic nor lose an item it accepted. It used to
// check for shutdown and then send on the queue without holding the lock across both,
// so a shutdown landing between the two closed the queue under the send and panicked
// with "send on closed channel".
func TestAddItemDuringShutdown(t *testing.T) {
	for run := 0; run < 200; run++ {
		var processed atomic.Int64
		fn := func(items []int) { processed.Add(int64(len(items))) }

		ctx, cancel := context.WithCancel(context.Background())
		var wg sync.WaitGroup
		b := NewBatcher[int](ctx, &wg, time.Millisecond, NewBatchMaxItems[int](10), fn)

		var accepted atomic.Int64
		var adders sync.WaitGroup
		start := make(chan struct{})
		for g := 0; g < 8; g++ {
			adders.Add(1)
			go func() {
				defer adders.Done()
				<-start
				for i := 0; i < 200; i++ {
					err := b.AddItem(i)
					if err == nil {
						accepted.Add(1)
						continue
					}
					require.ErrorIs(t, err, ErrNewItems)
				}
			}()
		}

		close(start)
		cancel()
		adders.Wait()
		wg.Wait()

		require.Equal(t, accepted.Load(), processed.Load(), "run %d: every accepted item is processed", run)
	}
}

// An AddItem blocked on a full queue when shutdown starts has to give up, or shutdown
// waits on it forever: the queue is only drained by the goroutine doing the shutting
// down.
func TestAddItemBlockedOnFullQueueDuringShutdown(t *testing.T) {
	release := make(chan struct{})
	fn := func(items []int) { <-release }

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	b := NewBatcher[int](ctx, &wg, time.Hour, NewBatchMaxItems[int](1), fn, WithQueueDepth(1))

	require.NoError(t, b.AddItem(1)) // taken by run, which then blocks in the handler
	require.Eventually(t, func() bool { return len(b.queue) == 0 }, time.Second, time.Millisecond)
	require.NoError(t, b.AddItem(2)) // fills the queue

	blocked := make(chan error, 1)
	go func() { blocked <- b.AddItem(3) }()
	time.Sleep(50 * time.Millisecond)

	cancel()
	close(release)

	select {
	case err := <-blocked:
		if err != nil {
			require.True(t, errors.Is(err, ErrNewItems))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("an AddItem blocked on a full queue never returned after shutdown")
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown never finished")
	}
}
