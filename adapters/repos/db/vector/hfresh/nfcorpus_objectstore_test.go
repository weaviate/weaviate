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

//go:build nfcorpus

package hfresh

import (
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
)

// objectStore serves the documents the way a shard does (the tests' default
// thunk hands out slices of the memory-mapped corpus, so every document read
// by a split, a merge, a reassign or the exact MaxSim costs nothing, where a
// shard reads the object from its objects bucket by the doc id's secondary
// key and unmarshals it, shard_read.go readMultiVectorByIndexIDIntoSlice). The
// store is on when TACHIOMFRESH_OBJECT_STORE names a directory: the bucket
// lives in <dir>/<cache name>-objects, an objects bucket of the shard's
// kind (Replace strategy, one secondary index, tombstones kept,
// shard_init_lsm.go initObjectBucket) with one storobj per document, its
// tokens as the multi-vector objectTargetVector. It is filled once (a
// count file marks it) and reused by every build of the run and by the
// TachiomFresh tests, which have the same code
// (tachiomfresh/objectstore_test.go). It is outside the index's
// directory, so disk_bytes stays the index's, and opened before the RAM
// baseline is taken, so its segments' bloom filters are not the index's
// RAM. Reads allocate per call, as the shard's do.
type objectStore struct {
	store  *lsmkv.Store
	bucket *lsmkv.Bucket
}

// objectTargetVector is the name the documents' multi-vector is stored
// under; the shard reads a target vector by name.
const objectTargetVector = "colbert"

// objectFlushEvery documents, the memtable is flushed, so a filled store
// has a few segments; the harness store has no flush cycle.
const objectFlushEvery = 8192

// openObjectStore returns the store for the corpus at cachePath, filled
// from doc when it is not yet, or nil when TACHIOMFRESH_OBJECT_STORE is
// unset.
func openObjectStore(t testing.TB, cachePath string, numDocs int, doc func(i int) [][]float32) *objectStore {
	root := os.Getenv("TACHIOMFRESH_OBJECT_STORE")
	if root == "" {
		return nil
	}
	dir := filepath.Join(root, strings.TrimSuffix(filepath.Base(cachePath), ".bin")+"-objects")
	require.NoError(t, os.MkdirAll(dir, 0o755))
	store := testinghelpers.NewDummyStoreFromFolder(dir, t)
	require.NoError(t, store.CreateOrLoadBucket(context.Background(), helpers.ObjectsBucketLSM,
		lsmkv.WithStrategy(lsmkv.StrategyReplace),
		lsmkv.WithSecondaryIndices(1),
		lsmkv.WithKeepTombstones(true)))
	o := &objectStore{store: store, bucket: store.Bucket(helpers.ObjectsBucketLSM)}

	marker := filepath.Join(dir, "count.txt")
	if b, err := os.ReadFile(marker); err == nil && strings.TrimSpace(string(b)) == strconv.Itoa(numDocs) {
		t.Logf("object store: %s, %d documents, reused\n", dir, numDocs)
	} else {
		start := time.Now()
		for i := range numDocs {
			require.NoError(t, o.put(uint64(i), doc(i)))
			if (i+1)%objectFlushEvery == 0 {
				require.NoError(t, store.FlushMemtables(context.Background()))
			}
		}
		require.NoError(t, store.FlushMemtables(context.Background()))
		require.NoError(t, os.WriteFile(marker, []byte(strconv.Itoa(numDocs)+"\n"), 0o644))
		t.Logf("object store: %s, %d documents, filled in %.1f s\n", dir, numDocs, time.Since(start).Seconds())
	}
	// three documents read back through the shard's path must be the
	// corpus's, filled now or reused from an earlier run
	for _, i := range []int{0, numDocs / 2, numDocs - 1} {
		got, err := o.thunk(context.Background(), uint64(i))
		require.NoError(t, err)
		require.Equal(t, doc(i), got, "object store: doc %d differs from the corpus", i)
		var container common.VectorSlice
		buffered, err := o.tempThunk(context.Background(), uint64(i), &container)
		require.NoError(t, err)
		require.Equal(t, doc(i), buffered, "object store: buffered read of doc %d differs", i)
	}
	return o
}

// objectUUID is the object id of document id, the same in every run so a
// filled store can be reused.
func objectUUID(id uint64) uuid.UUID {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], id)
	return uuid.NewSHA1(uuid.NameSpaceOID, b[:])
}

// put writes document id as a shard writes an object: the on-disk binary
// under the object's uuid, the doc id as the secondary key
// (shard_write_put.go upsertObjectDataLSM).
func (o *objectStore) put(id uint64, vecs [][]float32) error {
	obj := storobj.New(id)
	uid := objectUUID(id)
	obj.Object = models.Object{ID: strfmt.UUID(uid.String()), Class: "Doc"}
	obj.MultiVectors = map[string][][]float32{objectTargetVector: vecs}
	data, err := obj.MarshalBinaryDisk(false)
	if err != nil {
		return errors.Wrapf(err, "marshal doc %d", id)
	}
	docIDBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(docIDBytes, id)
	return o.bucket.Put(uid[:], data,
		lsmkv.WithSecondaryKey(helpers.ObjectsBucketLSMDocIDSecondaryIndex, docIDBytes))
}

// thunk reads document id as a shard does: the object by its doc id, the
// multi-vector out of its bytes. A missing object is an error, as there
// (the index treats a deleted document's error as the deletion).
func (o *objectStore) thunk(ctx context.Context, id uint64) ([][]float32, error) {
	keyBuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(keyBuf, id)
	bytes, _, err := o.bucket.GetBySecondaryWithBuffer(ctx, helpers.ObjectsBucketLSMDocIDSecondaryIndex, keyBuf, nil)
	if err != nil {
		return nil, err
	}
	if bytes == nil {
		return nil, errors.Errorf("no object for doc id %d", id)
	}
	return storobj.MultiVectorFromBinary(bytes, objectTargetVector)
}

// tempThunk reads document id as the shard's readMultiVectorByIndexIDIntoSlice
// does: the node's bytes into the container's byte buffer, the tokens into
// its backing array, both kept in the container for the next read. The
// same code as tachiomfresh's.
func (o *objectStore) tempThunk(ctx context.Context, id uint64, container *common.VectorSlice) ([][]float32, error) {
	if len(container.Buff8) < 8 {
		container.Buff8 = make([]byte, 8)
	}
	binary.LittleEndian.PutUint64(container.Buff8, id)
	bytes, newBuff, err := o.bucket.GetBySecondaryWithBuffer(ctx, helpers.ObjectsBucketLSMDocIDSecondaryIndex, container.Buff8, container.Buff)
	if err != nil {
		return nil, err
	}
	if bytes == nil {
		return nil, errors.Errorf("no object for doc id %d", id)
	}
	container.Buff = newBuff
	vecs, buffer, err := storobj.MultiVectorFromBinaryInto(bytes, container.Slice, objectTargetVector)
	if err != nil {
		return nil, err
	}
	container.Mem, container.Slice = buffer, buffer
	return vecs, nil
}

func (o *objectStore) close(t testing.TB) {
	require.NoError(t, o.store.Shutdown(context.Background()))
}
