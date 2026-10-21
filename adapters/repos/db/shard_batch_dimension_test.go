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

//go:build integrationTest

package db

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/entities/vectorindex/dynamic"
	"github.com/weaviate/weaviate/entities/vectorindex/flat"
	"github.com/weaviate/weaviate/entities/vectorindex/hfresh"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func TestShard_BatchDimensionValidation_GhostObjectPrevention(t *testing.T) {
	testCases := []struct {
		name  string
		uc    schemaConfig.VectorIndexConfig
		async bool
	}{
		{
			name:  "HNSW sync",
			uc:    hnsw.NewDefaultUserConfig(),
			async: false,
		},
		{
			name:  "HNSW async",
			uc:    hnsw.NewDefaultUserConfig(),
			async: true,
		},
		{
			name:  "Flat",
			uc:    flat.NewDefaultUserConfig(),
			async: false,
		},
		{
			name:  "Dynamic",
			uc:    dynamic.NewDefaultUserConfig(),
			async: true,
		},
		{
			name:  "HFresh",
			uc:    hfresh.NewDefaultUserConfig(),
			async: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			className := "BatchDimTest" + t.Name()
			class := &models.Class{Class: className}

			shd, idx := testShardWithSettings(t, ctx, class, tc.uc, false, tc.async)
			defer func() {
				require.Nil(t, idx.drop())
				require.Nil(t, os.RemoveAll(idx.Config.RootPath))
			}()

			testShard(t, context.Background(), class.Class)
			r := getRandomSeed()

			// Step 1: Establish dimension = 4 with an initial batch of 2 objects
			initialBatch := createRandomObjects(r, class.Class, 2, 4)
			errs := shd.PutObjectBatch(ctx, initialBatch)
			require.Len(t, errs, 2)
			for _, err := range errs {
				require.NoError(t, err)
			}
			require.Equal(t, 2, int(shd.Counter().Get()))

			if tc.async {
				waitForVectorQueue(t, shd, "")
			}

			// Step 2: Ingest a mixed batch containing:
			//   Obj 0: Valid (dim 4)
			//   Obj 1: Invalid (dim 5)
			//   Obj 2: Valid (dim 4)
			//   Obj 3: Invalid (dim 3)
			valid1 := createRandomObjects(r, class.Class, 1, 4)[0]
			invalid1 := createRandomObjects(r, class.Class, 1, 5)[0]
			valid2 := createRandomObjects(r, class.Class, 1, 4)[0]
			invalid2 := createRandomObjects(r, class.Class, 1, 3)[0]

			mixedBatch := []*storobj.Object{valid1, invalid1, valid2, invalid2}

			errs = shd.PutObjectBatch(ctx, mixedBatch)
			require.Len(t, errs, 4)

			// Step 3: Verify exact error messages on invalid objects and no error on valid objects
			require.NoError(t, errs[0])
			require.Error(t, errs[1])
			assert.Equal(t, `target vector "": dimension mismatch: expected 4, got 5`, errs[1].Error())
			require.NoError(t, errs[2])
			require.Error(t, errs[3])
			assert.Equal(t, `target vector "": dimension mismatch: expected 4, got 3`, errs[3].Error())

			// Step 4: Verify Ghost Object Prevention:
			// Rejected objects must NEVER be written to LSM store (not retrievable by ID)
			obj1Retrieved, err := shd.ObjectByID(ctx, invalid1.ID(), search.SelectProperties{}, additional.Properties{})
			require.NoError(t, err)
			assert.Nil(t, obj1Retrieved, "rejected object 1 must not exist in LSM storage")

			obj3Retrieved, err := shd.ObjectByID(ctx, invalid2.ID(), search.SelectProperties{}, additional.Properties{})
			require.NoError(t, err)
			assert.Nil(t, obj3Retrieved, "rejected object 2 must not exist in LSM storage")

			// Valid objects MUST exist in LSM store
			valid1Retrieved, err := shd.ObjectByID(ctx, valid1.ID(), search.SelectProperties{}, additional.Properties{})
			require.NoError(t, err)
			assert.NotNil(t, valid1Retrieved, "valid object 1 must exist in LSM storage")

			valid2Retrieved, err := shd.ObjectByID(ctx, valid2.ID(), search.SelectProperties{}, additional.Properties{})
			require.NoError(t, err)
			assert.NotNil(t, valid2Retrieved, "valid object 2 must exist in LSM storage")

			// Step 5: Verify vector search operates without error
			searchRes, _, err := shd.ObjectVectorSearch(ctx, []models.Vector{valid1.Vector}, []string{""}, 0, 10, nil, nil, nil, additional.Properties{}, nil, nil)
			require.NoError(t, err, "vector search must succeed after rejected batch")
			require.NotEmpty(t, searchRes)

			// Step 6: Subsequent batch with valid dimensions (dim 4) succeeds
			subsequentBatch := createRandomObjects(r, class.Class, 3, 4)
			subsequentErrs := shd.PutObjectBatch(ctx, subsequentBatch)
			require.Len(t, subsequentErrs, 3)
			for _, err := range subsequentErrs {
				require.NoError(t, err, "subsequent valid batch must succeed")
			}
		})
	}
}
