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

package postingedge

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/sroar"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/visited"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
)

var (
	// ErrNotBuilt is returned by a search on an index that holds vectors but
	// has not been built yet.
	ErrNotBuilt = errors.New("posting-edge index is not built yet")
	// ErrBuilt is returned by a write to an index that is already built.
	ErrBuilt = errors.New("posting-edge index is already built, it cannot change in this version")
	// ErrNotSupported is returned by the operations this version leaves out.
	ErrNotSupported = errors.New("not supported by this version of the posting-edge index")
)

// Index is the in-memory posting+edges index. It is filled through AddBatch
// by a single writer, turned into a searchable index by Build, and from then
// on serves any number of concurrent searches.
type Index struct {
	cfg                Config
	logger             logrus.FieldLogger
	provider           distancer.Provider
	cosine             bool
	tempVectors        *common.TempVectorsPool
	rescoreConcurrency int
	// rescoreFanouts counts the rescores that read their vectors in
	// goroutines of their own, which one with a budget of one does not.
	rescoreFanouts atomic.Int64

	// The search-time tunables, readable without a lock.
	ef            atomic.Int64
	rescoreLimit  atomic.Int64
	rescorePerK   atomic.Int64
	entryPostings atomic.Int64
	loadBatch     atomic.Int64
	sourceMargin  atomic.Uint32 // the bits of a float32

	dims atomic.Int32

	// mu serializes the writers: AddBatch, Build, config updates and
	// teardown. Searches never take it.
	mu      sync.Mutex
	uc      UserConfig
	pending *pendingVectors // nil once built
	closed  bool
	// shared carries the partition and the assignment from one build to
	// another, see buildShare. Nil outside benchmarks.
	shared *buildShare

	presentMu sync.RWMutex
	present   *sroar.Bitmap // ids of every vector added

	// state is everything a search reads. It is immutable once published.
	state atomic.Pointer[state]
}

// pendingVectors are the vectors added before the build, in input order.
type pendingVectors struct {
	ids     []uint64
	vectors [][]float32
}

// state is a built index.
type state struct {
	dims int
	// quantizer is the codec of the sidecar codes, which the edge builder
	// used too.
	quantizer codec
	// scanQuantizer is the codec of the codes in the postings when they have
	// codes of their own, nil when they share those of the sidecar.
	scanQuantizer codec
	centroids     *centroidIndex
	postings      *postingStore
	side          *sidecar
	visited       *visited.Pool
	scratch       sync.Pool
	build         BuildStats
	// deleted marks the vectors that were deleted and are still in their
	// postings, by the key the postings address them by: nil when there is
	// none. The scan of a posting skips a marked member, which is then no
	// candidate, is never returned, and has none of its edges followed.
	// Only the deletes experiment sets it (delete_test.go).
	deleted []bool
	// skipDeletedEndpoints leaves out the edges that lead to a marked
	// vector. Without it such an endpoint is scored as any other, from its
	// sidecar code, and can have its posting loaded: a routing hint.
	skipDeletedEndpoints bool
	// deletedAsSources has the scan score a marked member as any other, so
	// that it is a source when it is within the cut and its edges are
	// followed. It is still no candidate: it is not rescored, not returned,
	// and does not count towards the stop threshold.
	deletedAsSources bool
}

// New creates an empty index.
func New(cfg Config, uc UserConfig) (*Index, error) {
	if err := cfg.validate(); err != nil {
		return nil, errors.Wrap(err, "invalid config")
	}
	if err := uc.Validate(); err != nil {
		return nil, errors.Wrap(err, "invalid user config")
	}

	ix := &Index{
		cfg: cfg,
		logger: cfg.Logger.WithFields(logrus.Fields{
			"index_id": cfg.ID,
			"class":    cfg.ClassName,
			"shard":    cfg.ShardName,
		}),
		provider:    cfg.DistanceProvider,
		cosine:      cfg.DistanceProvider.Type() == "cosine-dot",
		tempVectors: common.NewTempVectorsPool(),
		// Same fan-out as the HNSW rescore: it is sized for reads that
		// block on I/O.
		rescoreConcurrency: 2 * runtime.GOMAXPROCS(0),
		uc:                 uc,
		pending:            &pendingVectors{},
		present:            sroar.NewBitmap(),
	}
	ix.storeSearchConfig(uc)
	return ix, nil
}

// rescoreLimitFor is the rescore limit of a search for k results: the one of
// the index, raised to RescorePerK times k where that is more. A limit of 0,
// no rescore, stays 0.
func (ix *Index) rescoreLimitFor(k int) int {
	limit := int(ix.rescoreLimit.Load())
	if perK := int(ix.rescorePerK.Load()); limit > 0 && perK > 0 {
		limit = max(limit, perK*k)
	}
	return limit
}

func (ix *Index) storeSearchConfig(uc UserConfig) {
	ix.ef.Store(int64(uc.EF))
	ix.rescoreLimit.Store(int64(uc.RescoreLimit))
	ix.rescorePerK.Store(int64(uc.RescorePerK))
	ix.entryPostings.Store(int64(uc.EntryPostings))
	ix.loadBatch.Store(int64(uc.LoadBatch))
	margin := uc.SourceMargin
	if margin == SourceMarginAuto {
		margin = 0
		if ix.cosine {
			margin = DefaultCosineSourceMargin
		}
	}
	ix.sourceMargin.Store(math.Float32bits(margin))
}

func (ix *Index) Type() common.IndexType {
	return common.IndexTypePostingEdge
}

func (ix *Index) Add(ctx context.Context, id uint64, vector []float32) error {
	return ix.AddBatch(ctx, []uint64{id}, [][]float32{vector})
}

// AddBatch stages vectors for the build. The index keeps the vectors until
// Build has run: a copy for cosine, which normalizes, and the slices
// themselves otherwise, which the caller must then leave untouched.
func (ix *Index) AddBatch(ctx context.Context, ids []uint64, vectors [][]float32) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(ids) != len(vectors) {
		return errors.Errorf("ids and vectors sizes does not match")
	}
	if len(ids) == 0 {
		return errors.Errorf("addBatch called with empty lists")
	}

	ix.mu.Lock()
	defer ix.mu.Unlock()

	if ix.closed {
		return errors.New("posting-edge index is shut down")
	}
	if ix.pending == nil {
		return ErrBuilt
	}
	if len(ix.pending.ids)+len(ids) > maxVectors {
		return errors.Errorf("index cannot hold more than %d vectors", maxVectors)
	}

	dims := int(ix.dims.Load())
	if dims == 0 {
		dims = len(vectors[0])
	}
	if dims == 0 {
		return errors.New("cannot add a vector without dimensions")
	}
	var bytes int64
	for i, vec := range vectors {
		if len(vec) != dims {
			return errors.Wrapf(common.ErrWrongDimensions,
				"vector of id %d has length %d, the index holds vectors of length %d", ids[i], len(vec), dims)
		}
		if err := checkValues(vec); err != nil {
			return errors.Wrapf(err, "vector of id %d", ids[i])
		}
		bytes += int64(len(vec)) * 4
	}
	if err := ix.cfg.AllocChecker.CheckAlloc(bytes); err != nil {
		return errors.Wrapf(err, "add batch of %d vectors", len(vectors))
	}

	if err := ix.markPresent(ids); err != nil {
		return err
	}
	ix.dims.Store(int32(dims))
	for i, vec := range vectors {
		ix.pending.ids = append(ix.pending.ids, ids[i])
		ix.pending.vectors = append(ix.pending.vectors, ix.normalized(vec))
	}
	return nil
}

// markPresent records the ids as added, or changes nothing and fails if one
// of them was added before: that would be an update.
func (ix *Index) markPresent(ids []uint64) error {
	ix.presentMu.Lock()
	defer ix.presentMu.Unlock()

	batch := make(map[uint64]struct{}, len(ids))
	for _, id := range ids {
		_, repeated := batch[id]
		if repeated || ix.present.Contains(id) {
			return errors.Wrapf(ErrNotSupported, "id %d was already added, updating a vector", id)
		}
		batch[id] = struct{}{}
	}
	for _, id := range ids {
		ix.present.Set(id)
	}
	return nil
}

// normalized returns the vector as the index compares it: unit length for
// cosine, in a copy, and unchanged otherwise.
func (ix *Index) normalized(vec []float32) []float32 {
	if ix.cosine {
		return distancer.Normalize(vec)
	}
	return vec
}

func (ix *Index) ValidateBeforeInsert(vector []float32) error {
	dims := int(ix.dims.Load())
	if dims != 0 && dims != len(vector) {
		return errors.Wrapf(common.ErrWrongDimensions,
			"new vector has length %d, the index holds vectors of length %d", len(vector), dims)
	}
	return checkValues(vector)
}

// maxSquaredNorm bounds the vectors the index accepts: below it, the squared
// distance between any two vectors is at most four times as large and so
// still a finite float32.
const maxSquaredNorm = math.MaxFloat32 / 4

// checkValues rejects a vector that has no place in the index: one with NaN
// or infinite values, or one so long that distances to it overflow. Such a
// vector has no nearest centroid. It is refused when it is added, because
// accepted, it would fail the build of the whole index.
func checkValues(vec []float32) error {
	negated, err := distancer.NewDotProductProvider().SingleDist(vec, vec)
	if err != nil {
		return err
	}
	// Written so that a NaN norm fails the comparison too.
	if squaredNorm := -negated; !(squaredNorm <= maxSquaredNorm) {
		return errors.New("vector has non-finite values or a norm too large to compute distances with")
	}
	return nil
}

func (ix *Index) ContainsDoc(docID uint64) bool {
	ix.presentMu.RLock()
	defer ix.presentMu.RUnlock()
	return ix.present.Contains(docID)
}

func (ix *Index) Iterate(fn func(docID uint64) bool) {
	ix.presentMu.RLock()
	ids := ix.present.ToArray()
	ix.presentMu.RUnlock()

	for _, id := range ids {
		if !fn(id) {
			return
		}
	}
}

// Delete is not supported: a silent no-op would leave the vector searchable.
func (ix *Index) Delete(ids ...uint64) error {
	return errors.Wrap(ErrNotSupported, "delete")
}

// UpdateUserConfig applies a new config. The search-time tunables can change
// at any time. What shaped the build can only change before the build.
func (ix *Index) UpdateUserConfig(updated schemaConfig.VectorIndexConfig, callback func()) error {
	defer callback()

	parsed, ok := updated.(UserConfig)
	if !ok {
		return errors.Errorf("config is not a posting-edge UserConfig, but %T", updated)
	}
	if err := parsed.Validate(); err != nil {
		return errors.Wrap(err, "invalid user config")
	}

	ix.mu.Lock()
	defer ix.mu.Unlock()

	if parsed.Distance != ix.uc.Distance {
		return errors.Errorf("distance is immutable: attempted change from %q to %q",
			ix.uc.Distance, parsed.Distance)
	}
	if ix.pending == nil {
		built, next := ix.uc, parsed
		next.EF, next.RescoreLimit, next.RescorePerK = built.EF, built.RescoreLimit, built.RescorePerK
		next.EntryPostings, next.LoadBatch = built.EntryPostings, built.LoadBatch
		next.SourceMargin = built.SourceMargin
		if next != built {
			return errors.New("targetPostingSize, codeBits, scanCodeBits, edges, buildProbe, " +
				"rescueLimit and trainingLimit cannot change once the index is built")
		}
	}

	ix.uc = parsed
	ix.storeSearchConfig(parsed)
	return nil
}

func (ix *Index) Compressed() bool {
	return true
}

func (ix *Index) Multivector() bool {
	return false
}

func (ix *Index) CompressionStats() compressionhelpers.CompressionStats {
	if st := ix.state.Load(); st != nil {
		return st.quantizer.Stats()
	}
	return compressionhelpers.UncompressedStats{}
}

// QueryVectorDistancer returns the exact distance from the query to a doc,
// read through the full-precision vector thunk.
func (ix *Index) QueryVectorDistancer(queryVector []float32) common.QueryVectorDistancer {
	query := ix.normalized(queryVector)
	view := ix.cfg.GetViewThunk()
	return common.QueryVectorDistancer{
		DistanceFunc: func(docID uint64) (float32, error) {
			return ix.exactDistance(context.Background(), query, docID, view)
		},
		CloseFunc: view.ReleaseView,
	}
}

// SearchByVectorDistance is not supported yet.
func (ix *Index) SearchByVectorDistance(ctx context.Context, vector []float32, dist float32,
	maxLimit int64, allow helpers.AllowList,
) ([]uint64, []float32, error) {
	return nil, nil, errors.Wrap(ErrNotSupported, "search by distance")
}

// Preload has nothing to warm: the index holds no full-precision vectors.
func (ix *Index) Preload(id uint64, vector []float32) {}

// PostStartup has nothing to restore.
// TODO: restore the index from disk once it is persisted.
func (ix *Index) PostStartup(ctx context.Context) {}

// Flush has nothing to write.
// TODO: flush the posting store and the edge log once the index is persisted.
func (ix *Index) Flush() error {
	return nil
}

// ListFiles returns no files, the index has none.
// TODO: list the index files once the index is persisted.
func (ix *Index) ListFiles(ctx context.Context, basePath string) ([]string, error) {
	return nil, nil
}

// PrepareForBackup fails rather than let a backup complete without the index
// in it.
// TODO: implement together with persistence.
func (ix *Index) PrepareForBackup(ctx context.Context) error {
	return errors.Wrap(ErrNotSupported, "backup")
}

// ResumeAfterBackup has nothing to resume, PrepareForBackup never succeeds.
// TODO: implement together with persistence.
func (ix *Index) ResumeAfterBackup(ctx context.Context) error {
	return nil
}

// SnapshotMutableFiles fails for the same reason PrepareForBackup does.
// TODO: implement together with persistence.
func (ix *Index) SnapshotMutableFiles(ctx context.Context, basePath string, dstFor func(relPath string) string) ([]string, error) {
	return nil, errors.Wrap(ErrNotSupported, "backup")
}

// Shutdown releases the index. It must not run concurrently with searches.
func (ix *Index) Shutdown(ctx context.Context) error {
	return ix.teardown(ctx, func(c *centroidIndex) error { return c.shutdown(ctx) })
}

// Drop releases the index and removes its directory under Config.RootPath. It
// must not run concurrently with searches.
func (ix *Index) Drop(ctx context.Context) error {
	err := ix.teardown(ctx, func(c *centroidIndex) error { return c.drop(ctx) })
	if err != nil {
		return err
	}
	return errors.Wrap(os.RemoveAll(ix.dir()), "remove index directory")
}

// dir is the directory the index owns under Config.RootPath. This version
// keeps nothing of its own in it: it is where the centroid graph puts its
// (empty) commit log directory, and where a persisted version would live.
func (ix *Index) dir() string {
	return filepath.Join(ix.cfg.RootPath, ix.cfg.ID+".postingedge.d")
}

func (ix *Index) teardown(ctx context.Context, release func(*centroidIndex) error) error {
	ix.mu.Lock()
	defer ix.mu.Unlock()

	if ix.closed {
		return nil
	}
	ix.closed = true
	ix.pending = nil

	st := ix.state.Swap(nil)
	if st == nil {
		return nil
	}
	return errors.Wrap(release(st.centroids), "release centroid graph")
}

// BuildStats describes the build of the index. ok is false until it is built.
func (ix *Index) BuildStats() (stats BuildStats, ok bool) {
	st := ix.state.Load()
	if st == nil {
		return BuildStats{}, false
	}
	return st.build, true
}

// MemoryStats describes the memory held by the built index. ok is false until
// it is built.
func (ix *Index) MemoryStats() (stats MemoryStats, ok bool) {
	st := ix.state.Load()
	if st == nil {
		return MemoryStats{}, false
	}
	return st.memoryStats(), true
}

func (st *state) memoryStats() MemoryStats {
	p := st.postings
	var centroidBytes int64
	for _, vec := range st.centroids.vectors {
		centroidBytes += int64(len(vec)) * 4
	}
	stats := MemoryStats{
		Vectors:             p.vectors(),
		Postings:            p.postings(),
		Edges:               len(p.edges),
		PostingBytes:        int64(len(p.docIDs))*8 + int64(len(p.start))*4 + int64(len(st.side.postingOf))*4,
		CodeBytes:           int64(len(st.side.codes)) * 8,
		EdgeBytes:           int64(len(p.edges))*4 + int64(len(p.edgeStart))*4,
		CentroidVectorBytes: centroidBytes,
	}
	if st.scanQuantizer != nil {
		stats.ScanCodeBytes = int64(len(p.codes)) * 8
	}
	return stats
}
