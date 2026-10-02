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

package inverted

import (
	"context"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/stopwords"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/config"
)

const objectsByDocIDClass = "ObjectsByDocIDTest"

func docIDKey(docID uint64) []byte {
	key := make([]byte, 8)
	binary.LittleEndian.PutUint64(key, docID)
	return key
}

// objectName is the value of the one property every stored test object gets.
func objectName(docID uint64) string {
	return fmt.Sprintf("name-%d", docID)
}

// newObjectsBucketSearcher returns a searcher over a fresh objects bucket and
// the doc ids that were not tombstoned; corrupt doc ids get bytes the decoder rejects.
func newObjectsBucketSearcher(t *testing.T, numObjects int, deleted, corrupt []uint64) (*Searcher, []uint64) {
	t.Helper()
	logger, _ := test.NewNullLogger()
	dirName := t.TempDir()
	store, err := lsmkv.New(dirName, dirName, logger, nil, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Shutdown(context.Background())) })
	require.NoError(t, store.CreateOrLoadBucket(context.Background(), helpers.ObjectsBucketLSM,
		lsmkv.WithStrategy(lsmkv.StrategyReplace), lsmkv.WithSecondaryIndices(1), lsmkv.WithClassName(objectsByDocIDClass)))
	bucket := store.Bucket(helpers.ObjectsBucketLSM)

	isDeleted := make(map[uint64]bool, len(deleted))
	for _, docID := range deleted {
		isDeleted[docID] = true
	}
	isCorrupt := make(map[uint64]bool, len(corrupt))
	for _, docID := range corrupt {
		isCorrupt[docID] = true
	}
	alive := make([]uint64, 0, numObjects)
	for docID := range uint64(numObjects) {
		obj := storobj.Object{
			MarshallerVersion: 1,
			Object: models.Object{
				ID:         strfmt.UUID(uuid.NewString()),
				Class:      objectsByDocIDClass,
				Properties: map[string]interface{}{"name": objectName(docID)},
			},
			DocID: docID,
		}
		data, err := obj.MarshalBinary()
		require.NoError(t, err)
		if isCorrupt[docID] {
			data = []byte("not a marshalled object")
		}
		require.NoError(t, bucket.Put([]byte(obj.ID()), data, lsmkv.WithSecondaryKey(0, docIDKey(docID))))
		if isDeleted[docID] {
			require.NoError(t, bucket.Delete([]byte(obj.ID()), lsmkv.WithSecondaryKey(0, docIDKey(docID))))
			continue
		}
		alive = append(alive, docID)
	}

	bitmapFactory := roaringset.NewBitmapFactory(roaringset.NewBitmapBufPoolNoop(), func() uint64 { return uint64(numObjects) })
	getClass := func(string) *models.Class { return &models.Class{Class: objectsByDocIDClass} }
	searcher := NewSearcher(logger, store, getClass, nil, nil,
		stopwords.NewProvider(fakeStopwordDetector{}, nil), 2, func() bool { return false }, nil, "",
		config.DefaultQueryNestedCrossReferenceLimit, bitmapFactory)
	return searcher, alive
}

func TestSearcherObjectsByDocID(t *testing.T) {
	type testCase struct {
		name       string
		numObjects int
		deleted    []uint64
		corrupt    []uint64
		properties []string
		refQuery   bool
		limit      int
		cancelCtx  bool
		wantCount  int
		wantErr    string
	}
	cases := []testCase{
		{name: "no doc ids", numObjects: 0, limit: 10, wantCount: 0},
		{name: "one doc id", numObjects: 1, limit: 10, wantCount: 1},
		{name: "limit above matches", numObjects: 20, limit: 50, wantCount: 20},
		{name: "limit equals matches", numObjects: 20, limit: 20, wantCount: 20},
		{name: "limit below matches", numObjects: 20, limit: 7, wantCount: 7},
		{name: "limit zero falls back to the query maximum", numObjects: 20, limit: 0, wantCount: 20},
		{name: "deleted doc ids do not count toward the limit", numObjects: 20, deleted: []uint64{1, 2, 3}, limit: 18, wantCount: 17},
		{name: "every doc id deleted", numObjects: 5, deleted: []uint64{0, 1, 2, 3, 4}, limit: 5, wantCount: 0},
		{
			name: "deleted doc ids inside the last bucket call", numObjects: 1100,
			deleted: []uint64{990, 991, 992, 993, 994, 995, 996, 997, 998, 999, 1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 1010},
			limit:   1000, wantCount: 1000,
		},
		{name: "cancelled context", numObjects: 20, limit: 10, cancelCtx: true},
		{name: "negative limit falls back to the query maximum", numObjects: 20, limit: -1, wantCount: 20},
		{
			// 600 doc ids fill the bucket's 16 lookup workers with shared props.
			name: "concurrent decodes share one property extraction", numObjects: 600,
			properties: []string{"name"}, limit: 600, wantCount: 600,
		},
		{
			// The uuid-only decode is a second decode path on the same workers.
			name: "concurrent uuid-only decodes", numObjects: 600, refQuery: true, limit: 600, wantCount: 600,
		},
		{
			name: "bytes the decoder rejects fail the call", numObjects: 20, corrupt: []uint64{7},
			limit: 20, wantErr: "unmarshal data object for doc id 7",
		},
		{
			name: "bytes the decoder rejects fail the call from a worker", numObjects: 600,
			corrupt: []uint64{499}, limit: 600, wantErr: "unmarshal data object for doc id 499",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			searcher, alive := newObjectsBucketSearcher(t, tc.numObjects, tc.deleted, tc.corrupt)
			docIDs := make([]uint64, tc.numObjects)
			for i := range docIDs {
				docIDs[i] = uint64(i)
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.cancelCtx {
				cancel()
			}

			got, err := searcher.objectsByDocID(ctx, newSliceDocIDsIterator(docIDs), additional.Properties{ReferenceQuery: tc.refQuery}, tc.limit, tc.properties)
			if tc.cancelCtx {
				require.ErrorIs(t, err, context.Canceled)
				return
			}
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				require.Nil(t, got)
				return
			}
			require.NoError(t, err)

			gotDocIDs := make([]uint64, len(got))
			for i, obj := range got {
				gotDocIDs[i] = obj.DocID
			}
			require.Equal(t, alive[:tc.wantCount], gotDocIDs)

			for _, prop := range tc.properties {
				for _, obj := range got {
					require.Equalf(t, objectName(obj.DocID), obj.Properties().(map[string]interface{})[prop],
						"doc id %d decoded property %q", obj.DocID, prop)
				}
			}
		})
	}
}
