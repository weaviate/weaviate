//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package db

import (
	"testing"

	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/stretchr/testify/assert"
)

func TestSearchByDistParams(t *testing.T) {
	s := Shard{
		index: &Index{
			Config: IndexConfig{
				QueryMaximumResults: config.DefaultQueryMaximumResults,
			},
		},
	}

	t.Run("param iteration", func(t *testing.T) {
		params := s.newSearchByDistParams(true)
		assert.Equal(t, 0, params.offset)
		assert.Equal(t, defaultSearchByDistInitialLimit, params.limit)
		assert.Equal(t, 100, params.totalLimit)

		params.iterate()
		assert.Equal(t, 100, params.offset)
		assert.Equal(t, 1000, params.limit)
		assert.Equal(t, 1100, params.totalLimit)
	})
}
