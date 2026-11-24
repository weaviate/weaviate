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

package dynamic

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/searchparams"
)

// paramsRecorder stands in for the active stage and records the params it
// receives; every other method is the nil embedded interface and unused.
type paramsRecorder struct {
	VectorIndex
	got []*searchparams.VectorSearchParams
}

func (r *paramsRecorder) SearchByVector(_ context.Context, _ []float32, _ int, _ helpers.AllowList,
	params *searchparams.VectorSearchParams,
) ([]uint64, []float32, error) {
	r.got = append(r.got, params)
	return nil, nil, nil
}

func (r *paramsRecorder) SearchByVectorDistance(_ context.Context, _ []float32, _ float32, _ int64,
	_ helpers.AllowList, params *searchparams.VectorSearchParams,
) ([]uint64, []float32, error) {
	r.got = append(r.got, params)
	return nil, nil, nil
}

func TestDynamicForwardsVectorSearchParams(t *testing.T) {
	params := &searchparams.VectorSearchParams{EF: 128, FilterStrategy: searchparams.FilterStrategyAcorn}
	stage := &paramsRecorder{}
	d := &dynamic{index: stage}

	_, _, err := d.SearchByVector(context.Background(), []float32{1, 2}, 10, nil, params)
	require.NoError(t, err)
	_, _, err = d.SearchByVectorDistance(context.Background(), []float32{1, 2}, 0.5, 100, nil, params)
	require.NoError(t, err)
	_, _, err = d.SearchByVector(context.Background(), []float32{1, 2}, 10, nil, nil)
	require.NoError(t, err)

	require.Len(t, stage.got, 3)
	require.Same(t, params, stage.got[0])
	require.Same(t, params, stage.got[1])
	require.Nil(t, stage.got[2])
}
