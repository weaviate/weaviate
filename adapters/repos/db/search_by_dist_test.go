package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSearchByDistParams(t *testing.T) {
	t.Run("param iteration", func(t *testing.T) {
		params := newSearchByDistParams()
		assert.Equal(t, 0, params.offset)
		assert.Equal(t, defaultSearchByDistInitialLimit, params.limit)
		assert.Equal(t, 100, params.totalLimit)

		params.iterate()
		assert.Equal(t, 100, params.offset)
		assert.Equal(t, 1000, params.limit)
		assert.Equal(t, 1100, params.totalLimit)
	})
}
