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

// singleVectorIndex implements VectorIndex but not VectorIndexMulti.
type singleVectorIndex struct{}

func (singleVectorIndex) AddBatch(context.Context, []uint64, [][]float32) error { return nil }
func (singleVectorIndex) ValidateBeforeInsert([]float32) error                  { return nil }

// A multi-vector record handed to an index without multi-vector support must
// be refused with an error, never panic.
func TestVectorRecord_MultiVectorOnIndexWithoutMultiSupport(t *testing.T) {
	record := &Vector[[][]float32]{ID: 1, Vector: [][]float32{{0.1, 0.2}, {0.3, 0.4}}}

	tests := []struct {
		name string
		run  func() error
	}{
		{name: "validate", run: func() error { return record.Validate(singleVectorIndex{}) }},
		{name: "add", run: func() error {
			return AddVectorsToIndex(context.Background(), []VectorRecord{record}, singleVectorIndex{})
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.run()
			require.ErrorIs(t, err, ErrMultiVectorUnsupported)
		})
	}
}
