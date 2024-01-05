//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

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
		assert.Equal(t, 0, p.Offset)
		assert.Equal(t, 25, p.Limit)
	})

	t.Run("with a offset present", func(t *testing.T) {
		p, err := ExtractPaginationFromArgs(map[string]interface{}{
			"offset": 11,
		})
		require.Nil(t, err)
		require.NotNil(t, p)
		assert.Equal(t, 11, p.Offset)
		assert.Equal(t, -1, p.Limit)
	})

	t.Run("with offset and limit present", func(t *testing.T) {
		p, err := ExtractPaginationFromArgs(map[string]interface{}{
			"offset": 11,
			"limit":  25,
		})
		require.Nil(t, err)
		require.NotNil(t, p)
		assert.Equal(t, 11, p.Offset)
		assert.Equal(t, 25, p.Limit)
	})
}
