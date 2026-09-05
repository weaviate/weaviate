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

package common

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// singleOnlyIndex implements VectorIndex but not VectorIndexMulti, like flat and dynamic
type singleOnlyIndex struct{}

func (i *singleOnlyIndex) AddBatch(ctx context.Context, ids []uint64, vector [][]float32) error {
	return nil
}
func (i *singleOnlyIndex) ValidateBeforeInsert(vector []float32) error { return nil }

// Multi-vector records routed to an index without multi-vector support must error, not panic
func TestMultiVectorOnSingleVectorIndex(t *testing.T) {
	index := &singleOnlyIndex{}
	record := &Vector[[][]float32]{ID: 1, Vector: [][]float32{{1, 2, 3}}}

	err := record.Validate(index)
	require.ErrorContains(t, err, "does not support multi vectors")

	err = AddVectorsToIndex(context.Background(), []VectorRecord{record}, index)
	require.ErrorContains(t, err, "does not support multi vectors")
}
