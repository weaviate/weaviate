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
	"encoding/binary"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/storobj"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func TestRepair_Tombstones_After_Interrupted_Cleanup(t *testing.T) {
	vectors := vectorsForDeleteTest()
	var index *hnsw
	var err error
	store := testinghelpers.NewDummyStore(t)
	defer store.Shutdown(context.Background())

	assert.Nil(t, store.CreateOrLoadBucket(context.Background(), helpers.ObjectsBucketLSM))
	for i := range vectors {
		if i == 3 {
			continue
		}
		idBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(idBytes, uint64(i))
		obj := storobj.New(uint64(i))
		obj.DocID = uint64(i)
		obj.Object.ID = strfmt.UUID(uuid.NewString())
		obj.Vector = vectors[i]
		bytes, err := obj.MarshalBinary()
		assert.Nil(t, err)
		store.Bucket(helpers.ObjectsBucketLSM).Put(idBytes, bytes)
	}

	t.Run("create control index", func(t *testing.T) {
		index, err = New(Config{
			RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
			ID:                    "delete-test",
			MakeCommitLoggerThunk: MakeNoopCommitLogger,
			DistanceProvider:      distancer.NewCosineDistanceProvider(),
			VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
				return vectors[int(id)], nil
			},
			TempVectorForIDThunk: TempVectorForIDThunk(vectors),
		}, ent.UserConfig{
			MaxConnections: 30,
			EFConstruction: 128,

			// The actual size does not matter for this test, but if it defaults to
			// zero it will constantly think it's full and needs to be deleted - even
			// after just being deleted, so make sure to use a positive number here.
			VectorCacheMaxObjects: 100000,
		}, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
			cyclemanager.NewCallbackGroupNoop(), store)
		require.Nil(t, err)

		// makes sure index is build only with level 0. To be removed after fixing WEAVIATE-179
		index.randFunc = func() float64 { return 0.1 }

		// to speed up test execution, size of nodes array is decreased
		// from default 25k to little over number of vectors
		index.nodes = make([]*vertex, int(1.2*float64(len(vectors))))

		for i, vec := range vectors {
			err := index.Add(uint64(i), vec)
			require.Nil(t, err)
		}
	})

	t.Run("repair detects deleted id", func(t *testing.T) {
		assert.Nil(t, index.RepairTombstones())
		assert.True(t, len(index.tombstones) == 1)
		assert.True(t, index.hasTombstone(3))
	})

	t.Run("destroy the index", func(t *testing.T) {
		require.Nil(t, index.Drop(context.Background()))
	})
}
