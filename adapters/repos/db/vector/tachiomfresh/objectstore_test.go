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

package tachiomfresh

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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
// HFresh + MUVERA tests, which have the same code
// (hfresh/nfcorpus_objectstore_test.go). It is outside the index's
// directory, so disk_bytes stays the index's, and opened before the RAM
// baseline is taken, so its segments' bloom filters are not the index's
// RAM. Reads allocate per call, as the shard's do.
type objectStore struct {
	store  *lsmkv.Store
	bucket *lsmkv.Bucket
	// dims is the corpus's token dimension, which token needs to compute a
	// token's range
	dims int
	// fetches counts the documents read through thunk (how many documents
	// a build fetches, against the documents it inserts); tokenReads the
	// tokens read through token, headRereads those whose first head of
	// storobj.MultiVectorLocatorHead bytes was short, readBytes the value
	// bytes both reads returned (heads included)
	fetches, tokenReads, headRereads, readBytes atomic.Int64
	// sim, when TACHIOMFRESH_OBJECT_STORE_SIM is set, simulates caches of
	// decoded documents of several sizes over the fetch stream: how many
	// fetches a bounded cache would have served
	sim *cacheSim
	// ids, when set, gives each document's token ids, which put stores
	// with the object, as a shard stores them (tokenIDs reads them back)
	ids func(i int) []uint32
}

// cacheSim is an LRU simulation per size: hits[i] counts the fetches
// that found their document among the sizes[i] distinct documents fetched
// most recently (the stack distance of the fetch). The distance is the
// number of documents whose last fetch lies after this document's last
// fetch, kept as marks in a Fenwick tree over the fetch sequence: one mark
// per document at its last fetch, moved on every fetch, prefix sums give
// the count. Logarithmic per fetch; the tree holds maxFetches positions.
type cacheSim struct {
	mu    sync.Mutex
	sizes []int
	hits  []int64
	total int64
	last  map[uint64]int32
	tree  []int32
	pos   int32
}

const simMaxFetches = 64 << 20

func newCacheSim(sizes []int) *cacheSim {
	return &cacheSim{sizes: sizes, hits: make([]int64, len(sizes)), last: make(map[uint64]int32), tree: make([]int32, simMaxFetches+1)}
}

func (c *cacheSim) add(i, delta int32) {
	for ; int(i) < len(c.tree); i += i & -i {
		c.tree[i] += delta
	}
}

func (c *cacheSim) sum(i int32) int32 {
	var s int32
	for ; i > 0; i -= i & -i {
		s += c.tree[i]
	}
	return s
}

func (c *cacheSim) see(doc uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if int(c.pos)+1 >= len(c.tree) {
		return // the tree is full: the simulation stops counting
	}
	c.pos++
	c.total++
	if at, ok := c.last[doc]; ok {
		distinct := int(c.sum(c.pos-1) - c.sum(at))
		for i, size := range c.sizes {
			if distinct < size {
				c.hits[i]++
			}
		}
		c.add(at, -1)
	}
	c.add(c.pos, 1)
	c.last[doc] = c.pos
}

func (c *cacheSim) report() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := fmt.Sprintf("cache simulation over %d fetches:", c.total)
	for i, size := range c.sizes {
		out += fmt.Sprintf(" size %d hit %.1f%%", size, 100*float64(c.hits[i])/float64(max(1, c.total)))
	}
	return out
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
	return openObjectStoreAt(t, objectStoreDir(root, cachePath), false, numDocs, doc, nil)
}

// openObjectStoreWithTokenIDs is openObjectStore for a store whose objects
// also hold their token ids, in a directory of its own
// (<root>/<cache name>-objects-tokenids), so the store without them is
// left as it is.
func openObjectStoreWithTokenIDs(t testing.TB, cachePath string, numDocs int, doc func(i int) [][]float32, ids func(i int) []uint32) *objectStore {
	root := os.Getenv("TACHIOMFRESH_OBJECT_STORE")
	if root == "" {
		return nil
	}
	return openObjectStoreAt(t, objectStoreDir(root, cachePath)+"-tokenids", false, numDocs, doc, ids)
}

// objectStoreDir is the store's directory for the corpus at cachePath:
// <root>/<cache name>-objects, shared by every run on that corpus.
func objectStoreDir(root, cachePath string) string {
	return filepath.Join(root, strings.TrimSuffix(filepath.Base(cachePath), ".bin")+"-objects")
}

// openObjectStoreAt opens the store in dir, filled from doc when it is not
// yet. With fresh, dir is removed first and filled again: the update test
// writes and drops objects as its rounds reinsert and delete, so it works on
// a copy of its own rather than on the store the search grids share.
func openObjectStoreAt(t testing.TB, dir string, fresh bool, numDocs int, doc func(i int) [][]float32, ids func(i int) []uint32) *objectStore {
	if fresh {
		require.NoError(t, os.RemoveAll(dir))
	}
	require.NoError(t, os.MkdirAll(dir, 0o755))
	store := testinghelpers.NewDummyStoreFromFolder(dir, t)
	require.NoError(t, store.CreateOrLoadBucket(context.Background(), helpers.ObjectsBucketLSM,
		lsmkv.WithStrategy(lsmkv.StrategyReplace),
		lsmkv.WithSecondaryIndices(1),
		lsmkv.WithKeepTombstones(true)))
	o := &objectStore{store: store, bucket: store.Bucket(helpers.ObjectsBucketLSM), ids: ids}
	if numDocs > 0 && len(doc(0)) > 0 {
		o.dims = len(doc(0)[0])
	}
	if os.Getenv("TACHIOMFRESH_OBJECT_STORE_SIM") != "" {
		o.sim = newCacheSim([]int{64, 256, 1024, 4096, 16384})
	}

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
	// corpus's, filled now or reused from an earlier run; their last token
	// read alone too
	for _, i := range []int{0, numDocs / 2, numDocs - 1} {
		got, err := o.thunk(context.Background(), uint64(i))
		require.NoError(t, err)
		require.Equal(t, doc(i), got, "object store: doc %d differs from the corpus", i)
		token, err := o.token(context.Background(), uint64(i), len(got)-1, 0)
		require.NoError(t, err)
		require.Equal(t, got[len(got)-1], token, "object store: token read of doc %d differs", i)
		var container common.VectorSlice
		buffered, err := o.tempThunk(context.Background(), uint64(i), &container)
		require.NoError(t, err)
		require.Equal(t, doc(i), buffered, "object store: buffered read of doc %d differs", i)
		if ids != nil {
			gotIDs, err := o.tokenIDs(context.Background(), uint64(i))
			require.NoError(t, err)
			require.Equal(t, ids(i), gotIDs, "object store: token ids of doc %d differ", i)
		}
	}
	o.fetches.Store(0)
	o.tokenReads.Store(0)
	o.headRereads.Store(0)
	o.readBytes.Store(0)
	return o
}

// counters reports the reads since the last call, as key=value pairs for a
// build line, and resets them.
func (o *objectStore) counters() string {
	return fmt.Sprintf("fetches=%d token_reads=%d head_rereads=%d read_bytes=%d",
		o.fetches.Swap(0), o.tokenReads.Swap(0), o.headRereads.Swap(0), o.readBytes.Swap(0))
}

// noReads is counters' line without an object store.
const noReads = "fetches=0 token_reads=0 head_rereads=0 read_bytes=0"

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
	if o.ids != nil {
		obj.Object.TokenIds = models.TokenIds{objectTargetVector: o.ids(int(id))}
	}
	data, err := obj.MarshalBinaryDisk(false)
	if err != nil {
		return errors.Wrapf(err, "marshal doc %d", id)
	}
	docIDBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(docIDBytes, id)
	return o.bucket.Put(uid[:], data,
		lsmkv.WithSecondaryKey(helpers.ObjectsBucketLSMDocIDSecondaryIndex, docIDBytes))
}

// delete drops document id's object as a shard does (shard_write_delete.go:
// a tombstone under the object's uuid, the doc id as the secondary key).
// A read of the id then fails, which the index treats as the deletion.
func (o *objectStore) delete(id uint64) error {
	uid := objectUUID(id)
	docIDBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(docIDBytes, id)
	return o.bucket.Delete(uid[:], lsmkv.WithSecondaryKey(helpers.ObjectsBucketLSMDocIDSecondaryIndex, docIDBytes))
}

// flush writes the memtable out, as the shard's flush cycle would (the
// harness store has none): the update test flushes after each batch of
// puts and deletes, so the objects it wrote sit in segments, as the filled
// store's do, and not in the heap it measures.
func (o *objectStore) flush() error {
	return o.store.FlushMemtables(context.Background())
}

// thunk reads document id as a shard does: the object by its doc id, the
// multi-vector out of its bytes. A missing object is an error, as there
// (the index treats a deleted document's error as the deletion).
func (o *objectStore) thunk(ctx context.Context, id uint64) ([][]float32, error) {
	o.fetches.Add(1)
	if o.sim != nil {
		o.sim.see(id)
	}
	keyBuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(keyBuf, id)
	bytes, _, err := o.bucket.GetBySecondaryWithBuffer(ctx, helpers.ObjectsBucketLSMDocIDSecondaryIndex, keyBuf, nil)
	if err != nil {
		return nil, err
	}
	if bytes == nil {
		return nil, errors.Errorf("no object for doc id %d", id)
	}
	o.readBytes.Add(int64(len(bytes)))
	return storobj.MultiVectorFromBinary(bytes, objectTargetVector)
}

// tokenIDs reads document id's token ids as the shard's tokenIDsByIndexID
// does: the object by its doc id, the token ids out of its bytes.
func (o *objectStore) tokenIDs(ctx context.Context, id uint64) ([]uint32, error) {
	keyBuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(keyBuf, id)
	bytes, err := o.bucket.GetBySecondary(ctx, helpers.ObjectsBucketLSMDocIDSecondaryIndex, keyBuf)
	if err != nil {
		return nil, err
	}
	if bytes == nil {
		return nil, errors.Errorf("no object for doc id %d", id)
	}
	return storobj.TokenIDsFromBinary(bytes, objectTargetVector)
}

// tempThunk reads document id as the shard's readMultiVectorByIndexIDIntoSlice
// does: the node's bytes into the container's byte buffer, the tokens into
// its backing array, both kept in the container for the next read. Counted
// as a document fetch.
func (o *objectStore) tempThunk(ctx context.Context, id uint64, container *common.VectorSlice) ([][]float32, error) {
	o.fetches.Add(1)
	if o.sim != nil {
		o.sim.see(id)
	}
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
	o.readBytes.Add(int64(len(bytes)))
	vecs, buffer, err := storobj.MultiVectorFromBinaryInto(bytes, container.Slice, objectTargetVector)
	if err != nil {
		return nil, err
	}
	container.Mem, container.Slice = buffer, buffer
	return vecs, nil
}

// token reads token pos of document id as the shard's readTokenByIndexID
// does: the object's first bytes by the doc id, the token's range picked
// from them, the floats from the range; the object is never copied or
// decoded. Allocates per call, as thunk does.
// dims is the index's, as the shard's reader takes it; the store's own
// dimension serves the tests that call token directly.
func (o *objectStore) token(ctx context.Context, id uint64, pos int, dims int) ([]float32, error) {
	if dims == 0 {
		dims = o.dims
	}
	o.tokenReads.Add(1)
	keyBuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(keyBuf, id)
	pick := func(head []byte) (uint64, uint64, uint64, error) {
		o.readBytes.Add(int64(len(head)))
		offset, length, need, err := storobj.MultiVectorTokenRange(head, objectTargetVector, pos, dims)
		if need > uint64(len(head)) {
			o.headRereads.Add(1)
		}
		return offset, length, need, err
	}
	bytes, _, err := o.bucket.GetBySecondaryRange(ctx, helpers.ObjectsBucketLSMDocIDSecondaryIndex, keyBuf, storobj.MultiVectorLocatorHead, pick, nil)
	if err != nil {
		return nil, err
	}
	if bytes == nil {
		return nil, errors.Errorf("no object for doc id %d", id)
	}
	o.readBytes.Add(int64(len(bytes)))
	return storobj.TokenFromBytes(bytes, dims, nil)
}

func (o *objectStore) close(t testing.TB) {
	require.NoError(t, o.store.Shutdown(context.Background()))
}

// BenchmarkObjectStoreRead splits the cost of one document fetch from the
// object store: the shard's lookup by doc id alone, the decoding of the
// object's bytes alone, and the two together as the thunk does them, on the
// store TACHIOMFRESH_OBJECT_STORE holds for TACHIOMFRESH_MVC1 (filled by a
// test run), random documents, warm.
//
//	TACHIOMFRESH_MVC1=... TACHIOMFRESH_OBJECT_STORE=... go test -tags=nfcorpus -run XXX -bench BenchmarkObjectStoreRead -benchtime 3s ./adapters/repos/db/vector/tachiomfresh/
func BenchmarkObjectStoreRead(b *testing.B) {
	path := os.Getenv("TACHIOMFRESH_MVC1")
	if path == "" || os.Getenv("TACHIOMFRESH_OBJECT_STORE") == "" {
		b.Skip("TACHIOMFRESH_MVC1 or TACHIOMFRESH_OBJECT_STORE not set")
	}
	corpus, err := LoadCorpus(path, 0)
	require.NoError(b, err)
	o := openObjectStore(b, path, corpus.NumDocs(), func(i int) [][]float32 { return corpus.Doc(i, nil) })
	require.NotNil(b, o)
	defer func() { require.NoError(b, o.store.Shutdown(context.Background())) }()
	rng := rand.New(rand.NewSource(1))
	ids := make([]uint64, 4096)
	for i := range ids {
		ids[i] = uint64(rng.Intn(corpus.NumDocs()))
	}
	ctx := context.Background()
	key := make([]byte, 8)

	b.Run("lookup", func(b *testing.B) {
		for i := 0; b.Loop(); i++ {
			binary.LittleEndian.PutUint64(key, ids[i&4095])
			bytes, _, err := o.bucket.GetBySecondaryWithBuffer(ctx, helpers.ObjectsBucketLSMDocIDSecondaryIndex, key, nil)
			if err != nil || bytes == nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("decode", func(b *testing.B) {
		binary.LittleEndian.PutUint64(key, ids[0])
		bytes, _, err := o.bucket.GetBySecondaryWithBuffer(ctx, helpers.ObjectsBucketLSMDocIDSecondaryIndex, key, nil)
		require.NoError(b, err)
		b.ResetTimer()
		for b.Loop() {
			if _, err := storobj.MultiVectorFromBinary(bytes, objectTargetVector); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("thunk", func(b *testing.B) {
		for i := 0; b.Loop(); i++ {
			if _, err := o.thunk(ctx, ids[i&4095]); err != nil {
				b.Fatal(err)
			}
		}
	})
	// the token read's parts. "lookup+head" is the ranged read with
	// a picker that reads nothing: the lookup and the first head read;
	// "token" the whole token thunk
	b.Run("lookup+head", func(b *testing.B) {
		nothing := func(head []byte) (uint64, uint64, uint64, error) { return 0, 0, 0, nil }
		for i := 0; b.Loop(); i++ {
			binary.LittleEndian.PutUint64(key, ids[i&4095])
			_, _, err := o.bucket.GetBySecondaryRange(ctx, helpers.ObjectsBucketLSMDocIDSecondaryIndex, key, storobj.MultiVectorLocatorHead, nothing, nil)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("token", func(b *testing.B) {
		for i := 0; b.Loop(); i++ {
			if _, err := o.token(ctx, ids[i&4095], 0, 0); err != nil {
				b.Fatal(err)
			}
		}
	})
	// the document read into a reused container
	b.Run("temp-thunk", func(b *testing.B) {
		var container common.VectorSlice
		for i := 0; b.Loop(); i++ {
			if _, err := o.tempThunk(ctx, ids[i&4095], &container); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("corpus-slice", func(b *testing.B) {
		var dst [][]float32
		for i := 0; b.Loop(); i++ {
			dst = corpus.Doc(int(ids[i&4095]), dst)
		}
	})
}
