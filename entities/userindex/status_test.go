package userindex

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStatus(t *testing.T) {
	s := &Status{}

	t.Run("insert initial index", func(t *testing.T) {
		err := s.Register("shard1", Index{
			ID:    "vector",
			Paths: []string{"shard1_path"},
		})
		require.Nil(t, err)

		expected := []Index{
			{
				ID:     "vector",
				Paths:  []string{"shard1_path"},
				shards: []string{"shard1"},
			},
		}

		assert.Equal(t, expected, s.List())
	})

	t.Run("try to insert same index/shard combination again", func(t *testing.T) {
		err := s.Register("shard1", Index{
			ID:    "vector",
			Paths: []string{"shard1_path"},
		})
		require.NotNil(t, err)

		expected := []Index{
			{
				ID:     "vector",
				Paths:  []string{"shard1_path"},
				shards: []string{"shard1"},
			},
		}

		assert.Equal(t, expected, s.List())
	})

	t.Run("insert independent index", func(t *testing.T) {
		err := s.Register("shard1", Index{
			ID:    "my_prop",
			Paths: []string{"shard1_my_prop"},
		})
		require.Nil(t, err)

		expected := []Index{
			{
				ID:     "vector",
				Paths:  []string{"shard1_path"},
				shards: []string{"shard1"},
			},
			{
				ID:     "my_prop",
				Paths:  []string{"shard1_my_prop"},
				shards: []string{"shard1"},
			},
		}

		assert.Equal(t, expected, s.List())
	})

	t.Run("merge new shard into existing", func(t *testing.T) {
		err := s.Register("shard2", Index{
			ID:    "my_prop",
			Paths: []string{"shard2_my_prop"},
		})
		require.Nil(t, err)

		expected := []Index{
			{
				ID:     "vector",
				Paths:  []string{"shard1_path"},
				shards: []string{"shard1"},
			},
			{
				ID:     "my_prop",
				Paths:  []string{"shard1_my_prop", "shard2_my_prop"},
				shards: []string{"shard1", "shard2"},
			},
		}

		assert.Equal(t, expected, s.List())
	})
}
