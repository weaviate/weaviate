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

package docid

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
)

var errScanFn = errors.New("scan fn failed")

func TestScanObjectsLSM(t *testing.T) {
	// enough objects that the scan splits them across several concurrent workers
	const manyObjects = 100

	tests := []struct {
		name        string
		objectCount int
		// the scanFn call that returns errScanFn, counted from one; 0 lets every
		// call succeed
		failOnCall int
	}{
		{name: "no objects", objectCount: 0, failOnCall: 0},
		{name: "every call succeeds", objectCount: manyObjects, failOnCall: 0},
		{name: "the only object fails", objectCount: 1, failOnCall: 1},
		{name: "the first scan call fails", objectCount: manyObjects, failOnCall: 1},
		{name: "a mid-scan call fails", objectCount: manyObjects, failOnCall: manyObjects / 2},
		{name: "the last scan call fails", objectCount: manyObjects, failOnCall: manyObjects},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			store, pointers := storeWithObjects(t, tt.objectCount, logger)

			var calls atomic.Int64
			scan := func(_ *models.PropertySchema, _ uint64) error {
				if int(calls.Add(1)) == tt.failOnCall {
					return errScanFn
				}
				return nil
			}

			err := ScanObjectsLSM(store, pointers, scan, []string{"name"}, logger)

			if tt.failOnCall == 0 {
				require.NoError(t, err)
				require.Equal(t, int64(tt.objectCount), calls.Load())
				return
			}
			require.ErrorIs(t, err, errScanFn)
		})
	}
}

// storeWithObjects returns a store holding count objects keyed by their UUID,
// with the docID secondary index ScanObjectsLSM resolves pointers through, and
// the docIDs of those objects.
func storeWithObjects(t *testing.T, count int, logger logrus.FieldLogger) (*lsmkv.Store, []uint64) {
	ctx := context.Background()
	dir := t.TempDir()

	store, err := lsmkv.New(dir, dir, logger, nil, nil,
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Shutdown(ctx)) })

	require.NoError(t, store.CreateOrLoadBucket(ctx, helpers.ObjectsBucketLSM,
		lsmkv.WithStrategy(lsmkv.StrategyReplace), lsmkv.WithSecondaryIndices(1)))
	bucket := store.Bucket(helpers.ObjectsBucketLSM)

	pointers := make([]uint64, count)
	for i := range pointers {
		docID := uint64(i)
		pointers[i] = docID

		id := uuid.New()
		obj := storobj.New(docID)
		obj.SetID(strfmt.UUID(id.String()))
		obj.Object.Properties = map[string]interface{}{"name": fmt.Sprintf("object-%d", i)}
		objBytes, err := obj.MarshalBinary()
		require.NoError(t, err)

		// the shard write path keys objects by the UUID's 16 raw bytes
		idBytes, err := id.MarshalBinary()
		require.NoError(t, err)

		docIDBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(docIDBytes, docID)
		require.NoError(t, bucket.Put(idBytes, objBytes,
			lsmkv.WithSecondaryKey(helpers.ObjectsBucketLSMDocIDSecondaryIndex, docIDBytes)))
	}

	return store, pointers
}
