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

package hnsw

import (
	"context"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// To prevent a regression on
// https://github.com/weaviate/weaviate/issues/1878
func Test_DynamicEF(t *testing.T) {
	type test struct {
		name       string
		config     ent.UserConfig
		limit      int
		expectedEf int
	}

	tests := []test{
		{
			name: "all defaults explicitly entered, limit: 100",
			config: ent.UserConfig{
				VectorCacheMaxObjects: 10,
				EF:                    -1,
				DynamicEFMin:          100,
				DynamicEFMax:          500,
				DynamicEFFactor:       8,
			},
			limit:      100,
			expectedEf: 500,
		},
		{
			name: "limit lower than min",
			config: ent.UserConfig{
				VectorCacheMaxObjects: 10,
				EF:                    -1,
				DynamicEFMin:          100,
				DynamicEFMax:          500,
				DynamicEFFactor:       8,
			},
			limit:      10,
			expectedEf: 100,
		},
		{
			name: "limit within the dynamic range",
			config: ent.UserConfig{
				VectorCacheMaxObjects: 10,
				EF:                    -1,
				DynamicEFMin:          100,
				DynamicEFMax:          500,
				DynamicEFFactor:       8,
			},
			limit:      23,
			expectedEf: 184,
		},
		{
			name: "explicit ef",
			config: ent.UserConfig{
				VectorCacheMaxObjects: 10,
				EF:                    78,
			},
			limit:      5,
			expectedEf: 78,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			index, err := New(Config{
				RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
				ID:                    "dynaimc-ef-test",
				MakeCommitLoggerThunk: MakeNoopCommitLogger,
				DistanceProvider:      distancer.NewCosineDistanceProvider(),
				VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
					return nil, errors.Errorf("not implemented")
				},
			}, test.config, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
				cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
			require.Nil(t, err)

			actualEF := index.searchTimeEF(test.limit)
			assert.Equal(t, test.expectedEf, actualEF)

			require.Nil(t, index.Drop(context.Background()))
		})
	}
}
