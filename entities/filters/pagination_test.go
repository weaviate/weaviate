package filters

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExtractPagination(t *testing.T) {
	t.Run("without a limit present", func(t *testing.T) {
		p, err := ExtractPaginationFromArgs(map[string]interface{}{})
		require.Nil(t, err)
		assert.Nil(t, p)
	})

	t.Run("with a limit present", func(t *testing.T) {
		p, err := ExtractPaginationFromArgs(map[string]interface{}{
			"limit": 25,
		})
		require.Nil(t, err)
		require.NotNil(t, p)
		assert.Equal(t, 25, p.Limit)
	})
}
