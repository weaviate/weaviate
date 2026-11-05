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

package db

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"
)

const groupByLimitTestClass = "GroupByLimitTest"

// TestGrouperJoinsExistingGroupAfterGroupsLimit exercises the storage
// grouping path in grouper.Do: once the groups limit is reached, a new value
// must not create another group, but the object still joins groups that
// already exist. With groups A and B established, an object carrying
// [C, A] must join A without creating C.
func TestGrouperJoinsExistingGroupAfterGroupsLimit(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	dirName := t.TempDir()
	store, err := lsmkv.New(dirName, dirName, logger, nil, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Shutdown(ctx)) })
	require.NoError(t, store.CreateOrLoadBucket(ctx, helpers.ObjectsBucketLSM,
		lsmkv.WithStrategy(lsmkv.StrategyReplace), lsmkv.WithSecondaryIndices(1), lsmkv.WithClassName(groupByLimitTestClass)))
	bucket := store.Bucket(helpers.ObjectsBucketLSM)

	tags := [][]string{{"A"}, {"B"}, {"C", "A"}}
	ids := make([]uint64, len(tags))
	dists := make([]float32, len(tags))
	for docID, docTags := range tags {
		obj := storobj.Object{
			MarshallerVersion: 1,
			Object: models.Object{
				ID:         strfmt.UUID(uuid.NewString()),
				Class:      groupByLimitTestClass,
				Properties: map[string]interface{}{"tags": docTags},
			},
			DocID: uint64(docID),
		}
		data, err := obj.MarshalBinary()
		require.NoError(t, err)
		docIDBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(docIDBytes, uint64(docID))
		require.NoError(t, bucket.Put([]byte(obj.ID()), data, lsmkv.WithSecondaryKey(0, docIDBytes)))
		ids[docID] = uint64(docID)
		dists[docID] = float32(docID) * 0.1
	}

	dt, err := (&schema.Schema{}).FindPropertyDataType([]string{"text[]"})
	require.NoError(t, err)

	grouper := newGrouper(ids, dists,
		&searchparams.GroupBy{Property: "tags", Groups: 2, ObjectsPerGroup: 5},
		bucket, dt, additional.Properties{}, []string{"tags"})

	objs, _, err := grouper.Do(ctx)
	require.NoError(t, err)
	require.Len(t, objs, 2, "value C must not create a third group")

	counts := map[string]int{}
	for _, obj := range objs {
		group, ok := obj.AdditionalProperties()["group"].(*additional.Group)
		require.True(t, ok, "grouped object must carry group info")
		counts[group.GroupedBy.Value] = group.Count
	}

	assert.Equal(t, 2, counts["A"], "group A must contain both objects carrying tag A")
	assert.Equal(t, 1, counts["B"], "group B must contain its single object")
	assert.NotContains(t, counts, "C", "group C must not be created past the groups limit")
}
