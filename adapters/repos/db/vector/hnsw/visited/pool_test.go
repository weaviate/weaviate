//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package visited_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/visited"
)

func TestPool_ReusesAndResetsSparseSet(t *testing.T) {
	p := visited.NewPool(1000)

	s := p.Borrow()
	s.Visit(42)

	assert.True(t, s.Visited(42))

	p.Return(s)

	s2 := p.Borrow()

	assert.False(t, s2.Visited(42))
}
