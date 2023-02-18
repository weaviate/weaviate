//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package userindex

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
)

func TestStatus(t *testing.T) {
	s := &Status{}

	t.Run("insert initial index", func(t *testing.T) {
		err := s.Register("shard1", Index{
			ID:      "vector",
			Paths:   []string{"shard1_path"},
			Reason:  "A good one",
			Status:  "ready",
			Type:    "tipo 00 (great for pizza)",
			Subject: "yo mama",
		})
		require.Nil(t, err)

		expected := []Index{
			{
				ID:      "vector",
				Paths:   []string{"shard1_path"},
				shards:  []string{"shard1"},
				Reason:  "A good one",
				Status:  "ready",
				Type:    "tipo 00 (great for pizza)",
				Subject: "yo mama",
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
				ID:      "vector",
				Paths:   []string{"shard1_path"},
				shards:  []string{"shard1"},
				Reason:  "A good one",
				Status:  "ready",
				Type:    "tipo 00 (great for pizza)",
				Subject: "yo mama",
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
				ID:      "vector",
				Paths:   []string{"shard1_path"},
				shards:  []string{"shard1"},
				Reason:  "A good one",
				Status:  "ready",
				Type:    "tipo 00 (great for pizza)",
				Subject: "yo mama",
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
				ID:      "vector",
				Paths:   []string{"shard1_path"},
				shards:  []string{"shard1"},
				Reason:  "A good one",
				Status:  "ready",
				Type:    "tipo 00 (great for pizza)",
				Subject: "yo mama",
			},
			{
				ID:     "my_prop",
				Paths:  []string{"shard1_my_prop", "shard2_my_prop"},
				shards: []string{"shard1", "shard2"},
			},
		}

		assert.Equal(t, expected, s.List())
	})

	t.Run("convert to swagger", func(t *testing.T) {
		actual := s.ToSwagger()
		expected := &models.IndexStatusList{
			ShardCount: 2,
			Total:      2,
			Indexes: []*models.IndexStatus{
				{
					ID:      "vector",
					Paths:   []string{"shard1_path"},
					Reason:  "A good one",
					Status:  "ready",
					Type:    "tipo 00 (great for pizza)",
					Subject: "yo mama",
				},
				{
					ID:    "my_prop",
					Paths: []string{"shard1_my_prop", "shard2_my_prop"},
				},
			},
		}

		assert.Equal(t, expected, actual)
	})

	t.Run("remove shard completely", func(t *testing.T) {
		s.RemoveShard("shard1")

		expected := []Index{
			{
				ID:     "my_prop",
				Paths:  []string{"shard2_my_prop"},
				shards: []string{"shard2"},
			},
		}

		assert.Equal(t, expected, s.List())
	})
}
