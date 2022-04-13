package hnsw

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSearchByDistParams(t *testing.T) {
	t.Run("param iteration", func(t *testing.T) {
		params := newSearchByDistParams(100)
		assert.Equal(t, 0, params.offset)
		assert.Equal(t, DefaultSearchByDistInitialLimit, params.limit)
		assert.Equal(t, 100, params.totalLimit)

		params.iterate()
		assert.Equal(t, 100, params.offset)
		assert.Equal(t, 1000, params.limit)
		assert.Equal(t, 1100, params.totalLimit)
	})
}
