package batcher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBatchMaxItems(t *testing.T) {
	const max = 5

	b := NewBatchMaxItems[int](max)

	t.Run("basic operation", func(t *testing.T) {
		require.Equal(t, 0, b.Len())
		b.Add(10)
		require.Equal(t, 1, b.Len())
		items := b.Items()
		require.Len(t, items, 1)
		require.Equal(t, 0, b.Len())
	})

	t.Run("fill up", func(t *testing.T) {
		require.Equal(t, 0, b.Len())

		for i := 0; i < max; i++ {
			result := b.Add(i)
			require.Equal(t, !(i < max-1), result)
		}

		items := b.Items()
		require.Len(t, items, max)
		require.Equal(t, 0, b.Len())
	})

	t.Run("overflow", func(t *testing.T) {
		require.Equal(t, 0, b.Len())

		for i := 0; i < max+1; i++ {
			result := b.Add(i)
			require.Equal(t, !(i < max-1), result)
		}

		items := b.Items()
		require.Len(t, items, max)
		require.Equal(t, 1, b.Len())
		require.Equal(t, []int{0, 1, 2, 3, 4}, items)

		items = b.Items()
		require.Len(t, items, 1)
		require.Equal(t, 0, b.Len())
		require.Equal(t, []int{5}, items)
	})
}
