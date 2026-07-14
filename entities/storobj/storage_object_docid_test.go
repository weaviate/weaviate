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

package storobj

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/bits"
	"math/rand"
	"sort"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
)

// fakeObjectsBucket is an in-memory stand-in for the objects LSM bucket. It maps
// doc IDs to marshalled object payloads; a missing doc ID resolves to nil (the
// WithEmpty "not found" case). The secondary-key lookup decodes the little-endian
// doc-id key exactly as the production write/read path encodes it, so the sort
// order under test is exercised end to end.
type fakeObjectsBucket struct {
	class string
	store map[uint64][]byte
}

func (b *fakeObjectsBucket) lookup(_ context.Context, _ int, seckey, buffer []byte) ([]byte, []byte, error) {
	id := binary.LittleEndian.Uint64(seckey)
	v, ok := b.store[id]
	if !ok {
		return nil, buffer, nil
	}
	return v, buffer, nil
}

func (b *fakeObjectsBucket) GetBySecondary(ctx context.Context, pos int, seckey []byte) ([]byte, error) {
	v, _, err := b.lookup(ctx, pos, seckey, nil)
	return v, err
}

func (b *fakeObjectsBucket) GetBySecondaryWithBuffer(ctx context.Context, pos int, seckey, buffer []byte) ([]byte, []byte, error) {
	return b.lookup(ctx, pos, seckey, buffer)
}

func (b *fakeObjectsBucket) ClassName() (string, error) {
	return b.class, nil
}

func (b *fakeObjectsBucket) SecondaryViewLookup() (secondaryLookup, func()) {
	return b.lookup, func() {}
}

// GetBySecondaryBatch resolves each key positionally, decoding the little-endian
// doc-id key exactly as the production path encodes it. Results align to keys; a
// missing doc id yields nil (the WithEmpty "not found" case).
func (b *fakeObjectsBucket) GetBySecondaryBatch(ctx context.Context, pos int, keys [][]byte) ([][]byte, error) {
	out := make([][]byte, len(keys))
	for i, key := range keys {
		v, _, err := b.lookup(ctx, pos, key, nil)
		if err != nil {
			return nil, err
		}
		out[i] = v
	}
	return out, nil
}

// marshalObjectForDocID builds a valid marshalled object carrying docID, so that
// after resolution out[i].DocID equals the requested id (the DocID is stored in
// the payload, not derived from the lookup key).
func marshalObjectForDocID(t *testing.T, class string, docID uint64) []byte {
	t.Helper()
	obj := &Object{
		MarshallerVersion: 1,
		DocID:             docID,
		Object: models.Object{
			// deterministic per-docID UUID so MarshalBinary accepts it
			ID:    strfmt.UUID(uuid.NewSHA1(uuid.NameSpaceOID, []byte(fmt.Sprintf("doc-%d", docID))).String()),
			Class: class,
		},
	}
	data, err := obj.MarshalBinary()
	require.NoError(t, err)
	return data
}

// newFakeBucket builds a fake bucket populated with objects for present, and no
// entry for the ids in missing.
func newFakeBucket(t *testing.T, class string, present []uint64) *fakeObjectsBucket {
	t.Helper()
	store := make(map[uint64][]byte, len(present))
	for _, id := range present {
		store[id] = marshalObjectForDocID(t, class, id)
	}
	return &fakeObjectsBucket{class: class, store: store}
}

// TestObjectsByDocIDWithEmptyPositionalRestore is the load-bearing round-trip:
// after the internal sort + permutation restore, out[i].DocID must equal ids[i]
// for every present id, and out[i] must be nil at every missing position. A
// permutation bug here is a silent customer-visible ranking misalignment
// (bm25_searcher_block.go indexes out[i] against ids[startAt+i]).
func TestObjectsByDocIDWithEmptyPositionalRestore(t *testing.T) {
	const class = "TestClass"
	logger := logrus.New()

	// A doc-id set deliberately chosen so numeric order differs from on-disk
	// (little-endian byte) order, plus enough ids to fan out across chunks.
	rng := rand.New(rand.NewSource(1))
	n := 400
	ids := make([]uint64, n)
	for i := range ids {
		// spread across byte lanes so the low byte (most significant under the
		// little-endian comparator) varies independently of the numeric value
		hi := uint64(rng.Intn(1 << 20))
		lo := uint64(rng.Intn(256))
		ids[i] = hi<<8 | lo
	}
	// dedupe to keep the "present vs missing" bookkeeping unambiguous
	ids = dedupe(ids)
	n = len(ids)

	// make roughly every third id missing (nil expected in place)
	present := make([]uint64, 0, n)
	missing := make(map[int]bool)
	for i, id := range ids {
		if i%3 == 0 {
			missing[i] = true
			continue
		}
		present = append(present, id)
	}
	bucket := newFakeBucket(t, class, present)

	out, err := ObjectsByDocIDWithEmpty(bucket, ids, additional.Properties{}, nil, logger)
	require.NoError(t, err)
	require.Len(t, out, n, "WithEmpty must preserve length")

	for i, id := range ids {
		if missing[i] {
			assert.Nil(t, out[i], "missing id at position %d must stay nil", i)
			continue
		}
		require.NotNil(t, out[i], "present id at position %d must resolve", i)
		assert.Equal(t, id, out[i].DocID,
			"position %d: resolved DocID %d != requested id %d (permutation restore broken)",
			i, out[i].DocID, id)
	}
}

// TestObjectsByDocIDCompactionPreservesInputOrder verifies the non-WithEmpty path:
// found objects are returned in caller (input) order with nils dropped, unchanged
// by the internal sort.
func TestObjectsByDocIDCompactionPreservesInputOrder(t *testing.T) {
	const class = "TestClass"
	logger := logrus.New()

	ids := []uint64{5, 256, 1, 1 << 40, 2, 300, 7, 1<<32 + 9}
	// present: drop 256 and 300 to force gaps
	present := []uint64{5, 1, 1 << 40, 2, 7, 1<<32 + 9}
	bucket := newFakeBucket(t, class, present)

	out, err := ObjectsByDocID(bucket, ids, additional.Properties{}, nil, logger)
	require.NoError(t, err)

	var wantOrder []uint64
	presentSet := map[uint64]bool{}
	for _, id := range present {
		presentSet[id] = true
	}
	for _, id := range ids {
		if presentSet[id] {
			wantOrder = append(wantOrder, id)
		}
	}

	require.Len(t, out, len(wantOrder))
	for i := range out {
		require.NotNil(t, out[i])
		assert.Equal(t, wantOrder[i], out[i].DocID,
			"compacted output must stay in caller order at position %d", i)
	}
}

// TestObjectsByDocIDDuplicateIDsPositional pins that duplicate ids resolve
// independently and each caller position gets the correct object.
func TestObjectsByDocIDDuplicateIDsPositional(t *testing.T) {
	const class = "TestClass"
	logger := logrus.New()

	ids := []uint64{42, 7, 42, 1000, 7, 42}
	bucket := newFakeBucket(t, class, []uint64{42, 7, 1000})

	out, err := ObjectsByDocIDWithEmpty(bucket, ids, additional.Properties{}, nil, logger)
	require.NoError(t, err)
	require.Len(t, out, len(ids))
	for i, id := range ids {
		require.NotNil(t, out[i], "position %d", i)
		assert.Equal(t, id, out[i].DocID, "position %d", i)
	}
}

// TestSecondaryKeyLessMatchesDiskTreeOrder is the comparator gate (AC3): the sort
// order induced by secondaryKeyLess must equal the order segmentindex.DiskTree.Get
// actually traverses, which is bytes.Compare over the little-endian 8-byte
// encoding of the doc id (disk_tree.go:78 keyEqual := bytes.Compare(key, ...)).
// It must NOT equal numeric doc-id order.
func TestSecondaryKeyLessMatchesDiskTreeOrder(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	ids := make([]uint64, 2000)
	for i := range ids {
		ids[i] = rng.Uint64()
	}

	// Order under test: sort by secondaryKeyLess.
	byComparator := append([]uint64(nil), ids...)
	sort.SliceStable(byComparator, func(a, b int) bool {
		return secondaryKeyLess(byComparator[a], byComparator[b])
	})

	// Ground truth: sort by bytes.Compare over the exact little-endian encoding
	// the production path writes and DiskTree.Get compares.
	byDiskBytes := append([]uint64(nil), ids...)
	sort.SliceStable(byDiskBytes, func(a, b int) bool {
		return bytesCompareLE(byDiskBytes[a], byDiskBytes[b]) < 0
	})

	require.Equal(t, byDiskBytes, byComparator,
		"secondaryKeyLess order must match bytes.Compare over little-endian encoding (DiskTree traversal order)")

	// And it must differ from numeric order on a crafted case: 0x0000000000000100
	// (=256) vs 0x0000000000000001 (=1). Numerically 1 < 256, but little-endian
	// bytes put 256's low byte (0x00) before 1's low byte (0x01), so on disk
	// 256 sorts BEFORE 1.
	assert.True(t, secondaryKeyLess(256, 1),
		"little-endian order must place 256 before 1")
	assert.False(t, 256 < 1) // sanity: numeric order is the opposite
}

// bytesCompareLE compares two doc ids by bytes.Compare over their little-endian
// 8-byte encodings (the exact bytes the objects secondary index stores).
func bytesCompareLE(a, b uint64) int {
	var ab, bb [8]byte
	binary.LittleEndian.PutUint64(ab[:], a)
	binary.LittleEndian.PutUint64(bb[:], b)
	// bytes.Compare over fixed-width equal-length keys
	for i := 0; i < 8; i++ {
		switch {
		case ab[i] < bb[i]:
			return -1
		case ab[i] > bb[i]:
			return 1
		}
	}
	return 0
}

// TestSecondaryKeyLessIsReverseBytesOrder is a direct unit pin on the comparator's
// definition: it is the numeric order of the byte-reversed integer.
func TestSecondaryKeyLessIsReverseBytesOrder(t *testing.T) {
	rng := rand.New(rand.NewSource(11))
	for i := 0; i < 10000; i++ {
		a, b := rng.Uint64(), rng.Uint64()
		assert.Equal(t, bits.ReverseBytes64(a) < bits.ReverseBytes64(b), secondaryKeyLess(a, b))
	}
}

func dedupe(in []uint64) []uint64 {
	seen := make(map[uint64]bool, len(in))
	out := in[:0]
	for _, v := range in {
		if seen[v] {
			continue
		}
		seen[v] = true
		out = append(out, v)
	}
	return out
}
