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
	"encoding/binary"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/storobj"
	ent "github.com/weaviate/weaviate/entities/vectorindex/postingedge"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

// Persistent is the index as a shard runs it: the index that grows on disk
// (grow.go) behind the interface a shard drives every vector index through,
// with what a shard asks for and the experiments never had. It starts empty,
// takes vectors under doc ids that have no bound, deletes, is searched while
// it is written to, and comes back as it was when it is closed and opened
// again.
//
// An empty index cannot be the index that grows: its codes need the mean of
// the vectors, and its first postings and their edges a batch to be built
// from. So the first vectors are kept aside, in a bucket of their own, and
// searched exactly. Once there are BaseSize of them they are built into the
// base in batch, the build the experiments grew every index from, and from
// then on every vector goes in by the incremental path: the search for its
// edges, which only reads (growIndex.plan), and then its writes, in order
// (growIndex.apply).
//
// A deleted vector is marked and nothing else: it keeps its place in its
// posting and goes on routing, and no search returns it (growIndex.remove).
// Nothing takes it out, so the space and the work of a deleted vector stay.
//
// The first build, the search of an insert for its edges, a split: all of it
// runs inside the insert that needs it. One thing runs in the background, in
// a goroutine of the index (persistent_maintenance.go): the exact vectors are
// written again in the order of their postings every time the index doubles.
//
// The base is written with the edges of a posting in one row next to its
// members (v5.3) and the exact vectors by slot. An index that was written
// before, with a value per list of edges and the vectors at their doc ids,
// opens as what it is and stays so.
//
// What it does not do says so with an error: a filtered search, a search by
// distance, a backup. Multi-vectors and distances other than cosine are
// refused before an index exists, where a class is parsed.
type Persistent struct {
	cfg      PersistentConfig
	logger   logrus.FieldLogger
	provider distancer.Provider
	// dir holds the files of the index, and buildDir those of the batch
	// build of the base while it runs.
	dir, buildDir string
	store         *lsmkv.Store
	// staging holds the vectors that came before the base was built: doc id
	// -> the vector as the index stores it.
	staging *lsmkv.Bucket

	dims atomic.Int32

	// writeMu lets one writer in at a time: an insert, a delete, a flush, a
	// change of config, the teardown. An insert is its search and then its
	// writes, so nothing changes between the two.
	writeMu sync.Mutex
	// mu guards what a search reads. The searches hold it for reading, and
	// so does the search of an insert. Whatever changes what they read holds
	// it for writing: the writes of an insert, a delete, the build of the
	// base, the teardown.
	mu sync.RWMutex
	uc ent.UserConfig
	// g is the index once its base is built, nil before.
	g *growIndex
	// staged has the doc ids of the vectors kept aside, while there is no
	// base.
	staged map[uint64]struct{}
	// dirty tells that something changed since the last flush.
	dirty  bool
	closed bool
	// failed is the error of a write that stopped halfway. What it left in
	// the store is not known to be an index, so nothing reads it or adds to
	// it after that.
	failed error

	// What runs in the background (persistent_maintenance.go): wake tells it
	// to look for work, stopMaintenance ends it and waits for it.
	wake            chan struct{}
	stopMaintenance func()
	// passMu lets one pass over the vectors run at a time, whoever calls it:
	// two would write the files of the same generation. It is taken before
	// the two locks above.
	passMu sync.Mutex
	// layouts counts the passes over the vectors that ended, for whoever
	// waits for one, and layoutNotBefore is how many vectors the file must
	// hold before a pass is tried again after one that failed.
	layouts         atomic.Int64
	layoutNotBefore atomic.Int64
	// cleans counts the passes over the deleted vectors that ended, and
	// refreshes those that built edges again.
	cleans    atomic.Int64
	refreshes atomic.Int64
	// refreshing is the refresh of the edges that is on, and cleaning the
	// pass over the deleted vectors, nil when there is none. Whoever holds
	// writeMu reads and writes them.
	refreshing *growRefresh
	cleaning   *growClean
	// plans has the plans of the inserts, which every caller takes for the
	// vectors it is adding and gives back.
	plans sync.Pool
}

const (
	// DefaultBaseSize is how many vectors an empty index gathers before it
	// builds its first postings. It is what the mean of the quantizer is
	// taken over: the training limit of centered RQ.
	DefaultBaseSize = DefaultTrainingLimit
	// DefaultRoutingBits is the width of the code the postings are ranked
	// by, the one kept in memory (rq.cacheMode "routing"). Vectors whose
	// full code is no wider are ranked by the full code.
	DefaultRoutingBits = 512
	// DefaultSplitThreshold is the size past which a posting splits when
	// maxPostingSizeKB is automatic, and DefaultMeanPostingSize the mean it
	// leaves, which is the one the base is built with: every measurement of
	// the index has it.
	DefaultSplitThreshold  = 360
	DefaultMeanPostingSize = 236
	// minSplitThreshold is the floor of an explicit maxPostingSizeKB, in
	// vectors: the one HFresh has.
	minSplitThreshold = 192
	// The automatic rescore limit never goes under what was measured at
	// 1536 bits nor over what the rule gives at 384, the narrowest code
	// there is a figure for.
	minAutoRescoreLimit = 64
	maxAutoRescoreLimit = 1600
	// keysStep is what the room for doc ids grows by, at least.
	keysStep = 1 << 12
	// DefaultConsolidate is how many changes to the edges of a posting wait
	// before its row of edges is written again: the K of v5.1 and v5.3.
	DefaultConsolidate = 64

	growBucketPrefix = "postingedge_grow_"
)

// PersistentConfig is what the owner of a Persistent index derives.
type PersistentConfig struct {
	ID        string
	ShardName string
	ClassName string
	// RootPath is the directory of the shard. The index keeps its files in a
	// directory of its own under it (helpers.PostingEdgeDirName).
	RootPath         string
	Logger           logrus.FieldLogger
	DistanceProvider distancer.Provider
	AllocChecker     memwatch.AllocChecker
	// MakeBucketOptions gives the options of the buckets the index keeps in
	// the store of the shard.
	MakeBucketOptions lsmkv.MakeBucketOptions
	// BaseSize is how many vectors the index gathers before it builds its
	// first postings, DefaultBaseSize when 0.
	BaseSize int
	// Seed makes the base and the splits reproducible.
	Seed uint64
	// TombstoneCallbacks and CommitLogCallbacks are the cycles of the node
	// that keep an HNSW graph, for the graph over the centroids: the first
	// takes out for good the centroids of the postings that were split or
	// merged, and the second condenses and combines the files of its commit
	// log. Without them nothing does either. CommitLogThreshold, when
	// positive, is the size past which a file of that log is closed.
	TombstoneCallbacks cyclemanager.CycleCallbackGroup
	CommitLogCallbacks cyclemanager.CycleCallbackGroup
	CommitLogThreshold int64

	// listEdges has the base written as the first version of the index wrote
	// it: a value per list of edges, and the exact vectors at their doc ids.
	// Only the tests set it, to have an index of then to open.
	listEdges bool
	// noMaintenance leaves out everything the index does by itself, in the
	// background and in line with its writes, for the tests and the
	// experiments that call it themselves. noBackground leaves out only what
	// runs in the background: the writes still pay their share of the
	// passes that are on.
	noMaintenance bool
	noBackground  bool
}

func (c *PersistentConfig) validate() error {
	switch {
	case c.ID == "":
		return errors.New("id cannot be empty")
	case c.RootPath == "":
		return errors.New("rootPath cannot be empty")
	case c.DistanceProvider == nil:
		return errors.New("distanceProvider cannot be nil")
	case c.BaseSize < 0:
		return errors.Errorf("baseSize must not be negative, got %d", c.BaseSize)
	}
	// The public config refuses every other distance where a class is
	// parsed. This is for whoever gets here another way.
	if c.DistanceProvider.Type() != "cosine-dot" {
		return errors.Wrapf(ErrNotSupported, "distance %q: the index only supports cosine in this version",
			c.DistanceProvider.Type())
	}
	c.Logger = common.LoggerOrDiscard(c.Logger)
	if c.AllocChecker == nil {
		c.AllocChecker = memwatch.NewDummyMonitor()
	}
	if c.MakeBucketOptions == nil {
		c.MakeBucketOptions = lsmkv.MakeRegularBucketOptions
	}
	if c.BaseSize == 0 {
		c.BaseSize = DefaultBaseSize
	}
	return nil
}

// Open opens the index of the given id under the root path, with its buckets
// in the given store: the one that was there, or an empty one.
func Open(cfg PersistentConfig, uc ent.UserConfig, store *lsmkv.Store) (_ *Persistent, err error) {
	if err := cfg.validate(); err != nil {
		return nil, errors.Wrap(err, "invalid config")
	}
	if err := uc.Validate(); err != nil {
		return nil, errors.Wrap(err, "invalid user config")
	}
	if store == nil {
		return nil, errors.New("invalid config: store cannot be nil")
	}
	p := &Persistent{
		cfg: cfg,
		// Whoever opens the index gives it a logger that says which index it
		// is already.
		logger:   cfg.Logger,
		provider: cfg.DistanceProvider,
		dir:      filepath.Join(cfg.RootPath, helpers.PostingEdgeDirName(cfg.ID)),
		buildDir: filepath.Join(cfg.RootPath, helpers.PostingEdgeBuildDirName(cfg.ID)),
		store:    store,
		uc:       uc,
		staged:   map[uint64]struct{}{},
	}
	p.plans.New = func() any { return newGrowPlan() }
	ctx := context.Background()
	if err := os.MkdirAll(p.dir, 0o755); err != nil {
		return nil, errors.Wrap(err, "create the directory of the index")
	}
	// Whatever a build of the base left when it did not end.
	if err := os.RemoveAll(p.buildDir); err != nil {
		return nil, errors.Wrap(err, "remove what a build left")
	}

	_, statErr := os.Stat(filepath.Join(p.dir, diskMetaFile))
	switch {
	case statErr == nil:
		// The base is written before the vectors it was built from are let
		// go: whatever is still kept aside is in the index already.
		if err := p.dropStaging(ctx); err != nil {
			return nil, err
		}
		if err := p.openGrown(ctx); err != nil {
			return nil, errors.Wrap(err, "open the index")
		}
	case os.IsNotExist(statErr):
		// Without a meta there is no base: what there is of one is of a
		// build that did not end, and the vectors it was built from are
		// still kept aside.
		if err := p.discardBase(ctx); err != nil {
			return nil, err
		}
		if err := p.openStaging(ctx); err != nil {
			return nil, err
		}
		if err := p.loadStaged(); err != nil {
			_ = p.closeStaging(ctx)
			return nil, err
		}
	default:
		return nil, errors.Wrap(statErr, "look for the meta of the index")
	}
	p.startMaintenance()
	return p, nil
}

// bucketName is the name in the store of the shard of a bucket of the index
// that grows.
func (p *Persistent) bucketName(bucket string) string {
	return helpers.PostingEdgeBucketName(strings.TrimPrefix(bucket, growBucketPrefix), p.cfg.ID)
}

func (p *Persistent) stagingName() string {
	return helpers.PostingEdgeBucketName(helpers.PostingEdgeBucketStaging, p.cfg.ID)
}

func (p *Persistent) growOpen() growOpen {
	return growOpen{
		store: p.store, makeOptions: p.cfg.MakeBucketOptions, names: p.bucketName, deletes: true,
		// A shard flushes its indexes and may stop without closing them:
		// what waits in memory of the edges is on disk too.
		keepPending: true,
		cycles: centroidCycles{
			tombstones: p.cfg.TombstoneCallbacks, commitLogs: p.cfg.CommitLogCallbacks,
			logThreshold: p.cfg.CommitLogThreshold,
		},
	}
}

func (p *Persistent) openStaging(ctx context.Context) error {
	name := p.stagingName()
	err := p.store.CreateOrLoadBucket(ctx, name, p.cfg.MakeBucketOptions(lsmkv.StrategyReplace)...)
	if err != nil {
		return errors.Wrapf(err, "open the bucket %s", name)
	}
	p.staging = p.store.Bucket(name)
	return nil
}

func (p *Persistent) closeStaging(ctx context.Context) error {
	if p.staging == nil {
		return nil
	}
	p.staging = nil
	return shutdownBucket(ctx, p.store, p.stagingName())
}

// loadStaged reads which vectors are kept aside, and their dimensions.
func (p *Persistent) loadStaged() error {
	err := growScan(p.staging, func(key, value []byte) error {
		if len(key) != 4 || len(value) == 0 || len(value)%4 != 0 {
			return errors.Errorf("a vector kept aside with a key of %d bytes and a value of %d", len(key), len(value))
		}
		dims := len(value) / 4
		if held := int(p.dims.Load()); held != 0 && held != dims {
			return errors.Errorf("vectors of %d and of %d dimensions kept aside", held, dims)
		}
		p.dims.Store(int32(dims))
		p.staged[uint64(binary.BigEndian.Uint32(key))] = struct{}{}
		return nil
	})
	return errors.Wrap(err, "read the vectors kept aside")
}

// dropStaging lets go of every vector kept aside, for good: their bucket
// leaves the store and its directory is removed. Deleting them one by one
// would leave their bytes in the segments of the bucket, under their
// tombstones, for as long as the store did not compact them away: a copy of
// every vector of the base that nothing reads again. An index with a base
// keeps nothing aside, so the bucket is not opened again.
func (p *Persistent) dropStaging(ctx context.Context) error {
	name := p.stagingName()
	p.staging = nil
	if err := shutdownBucket(ctx, p.store, name); err != nil {
		return errors.Wrapf(err, "close the bucket %s", name)
	}
	if err := os.RemoveAll(filepath.Join(p.store.GetDir(), name)); err != nil {
		return errors.Wrapf(err, "remove the bucket %s", name)
	}
	clear(p.staged)
	return nil
}

// discardBase removes what there is of a base without a meta, in the
// directory of the index and in the store: the buckets of the index, which
// must not be open, with everything in them.
func (p *Persistent) discardBase(ctx context.Context) error {
	entries, err := os.ReadDir(p.dir)
	if err != nil {
		return errors.Wrap(err, "read the directory of the index")
	}
	for _, entry := range entries {
		if err := os.RemoveAll(filepath.Join(p.dir, entry.Name())); err != nil {
			return errors.Wrap(err, "remove what a build left")
		}
	}
	for _, name := range helpers.PostingEdgeBucketNames(p.cfg.ID) {
		if name == p.stagingName() {
			continue
		}
		if err := shutdownBucket(ctx, p.store, name); err != nil {
			return errors.Wrapf(err, "close the bucket %s", name)
		}
		if err := os.RemoveAll(filepath.Join(p.store.GetDir(), name)); err != nil {
			return errors.Wrapf(err, "remove the bucket %s", name)
		}
	}
	return nil
}

// searchConfig is the config of the search of the index: what a class
// configures, and the defaults of everything it does not. scanBits is the
// width of the code a posting is scanned by, 0 while there is none.
func searchConfig(uc ent.UserConfig, scanBits int) UserConfig {
	in := NewDefaultUserConfig()
	in.Distance = uc.Distance
	in.EF = uc.EF
	in.RescoreLimit = uc.RQ.RescoreLimit
	if in.RescoreLimit == ent.RescoreLimitAuto {
		in.RescoreLimit = autoRescoreLimit(scanBits)
	}
	return in
}

// autoRescoreLimit is the rescore limit of an index that was given none, by
// the width of the code its postings are scanned by: the larger of 64 and
// 200 x (768 / bits)^3, which is what leaves a recall of 0.99 within reach of
// ef alone on the two datasets there are curves for (integration-plan.md, "El
// valor automático de rq.rescoreLimit"). The rule is a proposal: its anchor,
// 200 at 768 bits, falls between two measured points. Under 384 bits there is
// no measurement at all and the rule grows without bound, so it stops there.
func autoRescoreLimit(scanBits int) int {
	if scanBits <= 0 {
		return minAutoRescoreLimit
	}
	ratio := 768 / float64(scanBits)
	limit := math.Ceil(200 * ratio * ratio * ratio)
	return int(min(max(limit, minAutoRescoreLimit), maxAutoRescoreLimit))
}

// splitThreshold is the size past which a posting splits, in vectors. The
// automatic one is the same at every dimension. An explicit size in KiB is
// converted as HFresh converts its own, so that the same figure means the
// same threshold in the two indexes: an entry is its code of a bit per
// dimension and 25 bytes more, and no threshold goes under 192 vectors.
func splitThreshold(maxPostingSizeKB, dims int) int {
	if maxPostingSizeKB == ent.MaxPostingSizeAuto {
		return DefaultSplitThreshold
	}
	entry := float64(dims)/8 + 8 + 1 + compressionhelpers.RQMetadataSize
	return max(int(math.Ceil(float64(maxPostingSizeKB)*1024/entry)), minSplitThreshold)
}

// keysFor is the room for doc ids an index gets to hold the given one: past
// it by a quarter, and by keysStep at least, so that the tables by doc id are
// not copied for every vector.
func keysFor(docID uint64) int {
	keys := docID + 1 + max(docID/4, keysStep)
	return int(min(keys, maxVectors))
}

// openGrown opens the index from its directory and its buckets.
func (p *Persistent) openGrown(ctx context.Context) error {
	g, err := openGrowIndex(ctx, p.logger, p.dir, p.provider, searchConfig(p.uc, 0), p.growOpen())
	if err != nil {
		return err
	}
	g.ix.storeSearchConfig(searchConfig(p.uc, scanBitsOf(&g.meta)))
	g.insertEF = p.uc.EFConstruction
	p.dims.Store(int32(g.meta.Dims))
	p.g = g
	return nil
}

// scanBitsOf is the width of the code the members of a posting carry: a code
// is a header word and its sign bits.
func scanBitsOf(meta *growMeta) int {
	return (meta.ScanCodeWords - 1) * 64
}

func (p *Persistent) Type() common.IndexType {
	return common.IndexTypePostingEdge
}

// usable tells why the index takes no call, if it does not.
func (p *Persistent) usable() error {
	if p.closed {
		return errors.New("posting-edge index is shut down")
	}
	if p.failed != nil {
		return errors.Wrap(p.failed, "posting-edge index stopped: a write to it failed halfway, "+
			"and what it left is not known to be an index")
	}
	return nil
}

func (p *Persistent) Add(ctx context.Context, id uint64, vector []float32) error {
	return p.AddBatch(ctx, []uint64{id}, [][]float32{vector})
}

// AddBatch adds the vectors, in their order. A doc id the index holds already
// is refused, with the whole batch: that would be an update, and the index
// has none. A shard gives a vector that changes a new doc id, and deletes the
// old one.
//
// Several callers add at once: the shared workers of the node each bring a
// part of what a queue holds, and so do the goroutines of an import. Once the
// index has its base, the searches of their inserts run next to each other,
// in the goroutines that call, and their writes one at a time (v5.2,
// bench-report.md, "inserts con búsquedas en paralelo"). A caller searches
// for insertWindow of its vectors and then writes them, so that with eight
// callers the search of an insert misses the thirty-two or so that have not
// written yet, the batch the experiments measured.
func (p *Persistent) AddBatch(ctx context.Context, ids []uint64, vectors [][]float32) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(ids) != len(vectors) {
		return errors.Errorf("ids and vectors sizes does not match")
	}
	if len(ids) == 0 {
		return errors.Errorf("addBatch called with empty lists")
	}

	p.writeMu.Lock()
	based, err := p.admit(ids, vectors)
	if err != nil || !based {
		defer p.writeMu.Unlock()
		if err != nil {
			return err
		}
		// Without a base the vectors are kept aside, one after another, and
		// the one that completes the base builds it. Those after it are
		// inserted, by this caller alone.
		for i, vec := range vectors {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := p.add(ctx, ids[i], distancer.Normalize(vec)); err != nil {
				return errors.Wrapf(err, "add vector of id %d", ids[i])
			}
			if err := p.payInLine(ctx, ids[i]); err != nil {
				return err
			}
		}
		p.wakeIfDue()
		return nil
	}
	p.writeMu.Unlock()

	for from := 0; from < len(ids); from += insertWindow {
		if err := ctx.Err(); err != nil {
			return err
		}
		to := min(from+insertWindow, len(ids))
		if err := p.addWindow(ctx, ids[from:to], vectors[from:to]); err != nil {
			return err
		}
	}
	return nil
}

// insertWindow is how many inserts of a caller search before they write.
const insertWindow = 4

// admit checks a batch before any of it goes in: its dimensions, its values,
// and that none of its doc ids is taken, in the index or twice in the batch.
// It gives the index room for its doc ids, and tells whether the index has
// its base. The caller holds writeMu.
func (p *Persistent) admit(ids []uint64, vectors [][]float32) (based bool, err error) {
	if err := p.usable(); err != nil {
		return false, err
	}
	dims := int(p.dims.Load())
	if dims == 0 {
		dims = len(vectors[0])
	}
	if dims == 0 {
		return false, errors.New("cannot add a vector without dimensions")
	}
	batch := make(map[uint64]struct{}, len(ids))
	for i, vec := range vectors {
		id := ids[i]
		if id >= maxVectors {
			return false, errors.Errorf("doc id %d: the index addresses doc ids up to %d", id, uint64(maxVectors-1))
		}
		if len(vec) != dims {
			return false, errors.Wrapf(common.ErrWrongDimensions,
				"vector of id %d has length %d, the index holds vectors of length %d", id, len(vec), dims)
		}
		if err := checkValues(vec); err != nil {
			return false, errors.Wrapf(err, "vector of id %d", id)
		}
		_, repeated := batch[id]
		if repeated || p.placed(id) {
			return false, errors.Wrapf(ErrNotSupported, "id %d was already added, updating a vector", id)
		}
		batch[id] = struct{}{}
	}
	p.dims.Store(int32(dims))
	if p.g == nil {
		return false, nil
	}
	if last := slices.Max(ids); last >= uint64(p.g.meta.Keys) {
		p.mu.Lock()
		err := p.g.extend(keysFor(last))
		p.mu.Unlock()
		if err != nil {
			return false, err
		}
	}
	return true, nil
}

// addWindow inserts a few vectors of a caller: their searches, which read
// and write nothing, next to the queries and to the searches of the other
// callers, and then their writes, in their order, with nobody else writing.
// A search does not see the vectors of its window, nor those the other
// callers have searched for and not written. What was written between a
// search and its writes, the writes look at again (growIndex.begin).
func (p *Persistent) addWindow(ctx context.Context, ids []uint64, vectors [][]float32) error {
	plans := make([]*growPlan, len(ids))
	for i := range plans {
		plans[i] = p.plans.Get().(*growPlan)
	}
	defer func() {
		for _, plan := range plans {
			p.plans.Put(plan)
		}
	}()

	p.mu.RLock()
	if err := p.usable(); err != nil {
		p.mu.RUnlock()
		return err
	}
	g := p.g
	for i, id := range ids {
		plans[i].work = nil
		if err := g.plan(ctx, plans[i], id, distancer.Normalize(vectors[i])); err != nil {
			p.mu.RUnlock()
			return errors.Wrapf(err, "add vector of id %d", id)
		}
	}
	p.mu.RUnlock()

	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	for i, plan := range plans {
		if err := p.write(ctx, g, plan); err != nil {
			return errors.Wrapf(err, "add vector of id %d", ids[i])
		}
		if err := p.payInLine(ctx, ids[i]); err != nil {
			return err
		}
	}
	p.wakeIfDue()
	return nil
}

// write applies the writes of an insert whose search is done. They are not
// left halfway because whoever asked stopped waiting. The caller holds
// writeMu.
func (p *Persistent) write(ctx context.Context, g *growIndex, plan *growPlan) error {
	ctx = context.WithoutCancel(ctx)
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.usable(); err != nil {
		return err
	}
	if p.placed(plan.docID) {
		// Another caller gave the doc id since this one was let in.
		return errors.Wrapf(ErrNotSupported, "id %d was already added, updating a vector", plan.docID)
	}
	if err := g.insertable(plan.docID, plan.vector); err != nil {
		return err
	}
	if g.outdated(plan) {
		// A vector left the index since the search: it runs again, with
		// nobody else in.
		if err := g.plan(ctx, plan, plan.docID, plan.vector); err != nil {
			return err
		}
	} else if err := g.refresh(ctx, plan); err != nil {
		// The posting the search chose was split since.
		return err
	}
	if err := g.apply(ctx, plan); err != nil {
		p.failed = err
		return err
	}
	p.dirty = true
	return nil
}

// payInLine has an insert pay its share of the passes that are on: of the
// refresh of the edges of the oldest vectors, and of the one that takes the
// deleted vectors out. The caller holds writeMu.
func (p *Persistent) payInLine(ctx context.Context, id uint64) error {
	if err := p.refreshInLine(ctx); err != nil {
		return errors.Wrapf(err, "refresh edges after the vector of id %d", id)
	}
	if err := p.cleanInLine(ctx, 1); err != nil {
		return errors.Wrapf(err, "take the deleted vectors out after the vector of id %d", id)
	}
	return nil
}

// wakeIfDue tells what runs in the background that there is work, if there
// is. The caller holds writeMu.
func (p *Persistent) wakeIfDue() {
	if p.g != nil && (p.g.layoutDue() || p.refreshing != nil || p.g.refreshDue() || p.cleaning != nil) {
		p.wakeMaintenance()
	}
}

// placed tells whether a doc id is taken: kept aside, or in a posting, where
// a deleted vector still is. The caller holds writeMu.
func (p *Persistent) placed(id uint64) bool {
	if p.g == nil {
		_, staged := p.staged[id]
		return staged
	}
	return id < uint64(len(p.g.live)) && p.g.live[id]
}

// add takes one vector, normalized. The caller holds writeMu.
func (p *Persistent) add(ctx context.Context, id uint64, vector []float32) error {
	g := p.g
	if g == nil {
		return p.stage(ctx, id, vector)
	}
	if id >= uint64(g.meta.Keys) {
		p.mu.Lock()
		err := g.extend(keysFor(id))
		p.mu.Unlock()
		if err != nil {
			return err
		}
	}
	if err := g.insertable(id, vector); err != nil {
		return err
	}

	// The search for its edges, which reads and writes nothing: it runs next
	// to the queries. This is the step that goes to the shared workers of
	// the node, several at once.
	plan := g.own
	plan.work = nil
	p.mu.RLock()
	err := g.plan(ctx, plan, id, vector)
	p.mu.RUnlock()
	if err != nil {
		return err
	}

	// Its writes, in order. No search runs next to them, and they are not
	// left halfway because whoever asked stopped waiting.
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := g.apply(context.WithoutCancel(ctx), plan); err != nil {
		p.failed = err
		return err
	}
	p.dirty = true
	return nil
}

// stage keeps a vector aside while there is no base, and builds the base
// once there are enough.
func (p *Persistent) stage(ctx context.Context, id uint64, vector []float32) error {
	raw := make([]byte, 4*len(vector))
	for i, x := range vector {
		putFloat32(raw[4*i:], x)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.staging.Put(growDocKey(id), raw); err != nil {
		return errors.Wrap(err, "keep the vector aside")
	}
	p.staged[id] = struct{}{}
	p.dirty = true
	if len(p.staged) < p.cfg.BaseSize {
		return nil
	}
	return errors.Wrap(p.buildBase(context.WithoutCancel(ctx)), "build the first postings")
}

// readStaged reads the vectors kept aside, in the order of their doc ids.
func (p *Persistent) readStaged() (ids []uint64, vectors [][]float32, err error) {
	dims := int(p.dims.Load())
	err = growScan(p.staging, func(key, value []byte) error {
		if len(key) != 4 || len(value) != 4*dims {
			return errors.Errorf("a vector kept aside with a key of %d bytes and a value of %d, for %d dimensions",
				len(key), len(value), dims)
		}
		vector := make([]float32, dims)
		for i := range vector {
			vector[i] = getFloat32(value[4*i:])
		}
		ids, vectors = append(ids, uint64(binary.BigEndian.Uint32(key))), append(vectors, vector)
		return nil
	})
	return ids, vectors, errors.Wrap(err, "read the vectors kept aside")
}

// buildBase builds the vectors kept aside into the base of the index, in
// batch, writes it and opens it. The base has a meta only once it is whole:
// if anything fails before, what was written is removed, the vectors stay
// aside, and the next vector tries again. The caller holds mu for writing.
func (p *Persistent) buildBase(ctx context.Context) (err error) {
	ids, vectors, err := p.readStaged()
	if err != nil {
		return err
	}
	if len(ids) != len(p.staged) {
		return errors.Errorf("%d vectors kept aside in the store, %d counted", len(ids), len(p.staged))
	}
	dims := int(p.dims.Load())
	threshold := splitThreshold(p.uc.MaxPostingSizeKB, dims)
	buildConfig := NewDefaultUserConfig()
	buildConfig.Distance = p.uc.Distance
	buildConfig.TargetPostingSize = max(1, threshold*DefaultMeanPostingSize/DefaultSplitThreshold)

	batch, err := New(Config{
		ID:               p.cfg.ID,
		ShardName:        p.cfg.ShardName,
		ClassName:        p.cfg.ClassName,
		RootPath:         p.buildDir,
		Logger:           p.cfg.Logger,
		DistanceProvider: p.provider,
		AllocChecker:     p.cfg.AllocChecker,
		// The build reads the vectors it was given, and nothing searches
		// what it builds.
		TempVectorForIDWithViewThunk: func(_ context.Context, id uint64, _ *common.VectorSlice, _ common.BucketView,
		) ([]float32, error) {
			return nil, storobj.NewErrNotFoundf(id, "the build of the base reads no vector by id")
		},
		GetViewThunk: func() common.BucketView { return noopBucketView{} },
		Seed:         p.cfg.Seed,
	}, buildConfig)
	if err != nil {
		return errors.Wrap(err, "configure the build")
	}
	defer func() {
		if dropped := batch.Drop(ctx, false); err == nil && dropped != nil {
			p.logger.Warnf("drop the build of the base: %v", dropped)
		}
		if removed := os.RemoveAll(p.buildDir); err == nil && removed != nil {
			p.logger.Warnf("remove the directory of the build of the base: %v", removed)
		}
	}()
	if err := batch.AddBatch(ctx, ids, vectors); err != nil {
		return err
	}
	if err := batch.Build(ctx); err != nil {
		return err
	}
	st := batch.state.Load()
	// The base is written with the vectors as they were kept aside,
	// normalized once, which is how every later insert writes its own. The
	// build normalized its copies again, as it does with whatever it is
	// given: its codes are of vectors that may differ from these in the last
	// bit, as they do wherever the experiments write a base.
	byID := make(map[uint64][]float32, len(ids))
	for i, id := range ids {
		byID[id] = vectors[i]
	}
	vectorOf := func(id uint64) []float32 { return byID[id] }

	// The code the postings are ranked by, which is the one that stays in
	// memory, where the full code is wider than it.
	if fullBits := (st.postings.codeWords - 1) * 64; fullBits > DefaultRoutingBits {
		routing, err := st.quantizer.withBits(DefaultRoutingBits)
		if err != nil {
			return errors.Wrap(err, "routing code")
		}
		if st, err = haloState(ctx, p.logger, st, routing, vectorOf); err != nil {
			return err
		}
	}

	cfg := growConfig{
		keys: keysFor(slices.Max(ids)), neighbors: buildConfig.Edges, threshold: threshold, seed: p.cfg.Seed,
		open: p.growOpen(),
	}
	if !p.cfg.listEdges {
		// The format the experiments left as the candidate (v5.3): the edges
		// of a posting in one row, next to its members, and the vectors by
		// slot, so that they can be laid out again as the index grows.
		cfg.consolidate, cfg.rowsWithMembers, cfg.slots = DefaultConsolidate, true, true
	}
	if err := writeGrowIndex(ctx, p.logger, p.dir, st, cfg, p.provider, vectorOf); err != nil {
		if discarded := p.discardBase(ctx); discarded != nil {
			p.logger.Errorf("remove a base that was not written whole: %v", discarded)
		}
		return errors.Wrap(err, "write the base")
	}
	if err := p.openGrown(ctx); err != nil {
		// The base is whole on disk, and it is what the index is from here
		// on: it opens when the shard does.
		p.failed = err
		return errors.Wrap(err, "open the base")
	}
	if err := p.dropStaging(ctx); err != nil {
		p.failed = err
		return err
	}
	p.logger.WithFields(logrus.Fields{
		"action":   "postingedge_build_base",
		"vectors":  len(ids),
		"postings": p.g.meta.Postings,
	}).Info("built the first postings of the index")
	return nil
}

// Delete marks the vectors deleted. A doc id the index never held, or one
// that is deleted already, is not an error: a shard deletes from every index
// it has, and more than once.
func (p *Persistent) Delete(ids ...uint64) error {
	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	if err := p.usable(); err != nil {
		return err
	}
	marks, err := p.mark(ids)
	if err != nil {
		return err
	}
	// While a pass takes the deleted vectors out, every delete pays its
	// share of it.
	if err := p.cleanInLine(context.Background(), marks); err != nil {
		return errors.Wrap(err, "take the deleted vectors out")
	}
	if p.g != nil && (p.cleaning != nil || p.g.cleanDue()) {
		p.wakeMaintenance()
	}
	return nil
}

// mark marks the vectors deleted, with no search in the index, and tells how
// many it marked. The caller holds writeMu.
func (p *Persistent) mark(ids []uint64) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	marks := 0
	for _, id := range ids {
		if p.g == nil {
			if _, staged := p.staged[id]; !staged {
				continue
			}
			// Nothing leads to a vector that is kept aside: it goes for good.
			if err := p.staging.Delete(growDocKey(id)); err != nil {
				return marks, errors.Wrapf(err, "delete doc %d", id)
			}
			delete(p.staged, id)
			p.dirty = true
			continue
		}
		marked, err := p.g.remove(id)
		if err != nil {
			return marks, errors.Wrapf(err, "delete doc %d", id)
		}
		if marked {
			marks++
			p.dirty = true
		}
	}
	return marks, nil
}

// SearchByVector returns the k nearest vectors to the query, nearest first.
// An index without vectors returns none.
func (p *Persistent) SearchByVector(ctx context.Context, vector []float32, k int,
	allow helpers.AllowList,
) ([]uint64, []float32, error) {
	// A filter that is dropped would return what the filter leaves out, and
	// nothing above the index checks.
	if allow != nil {
		return nil, nil, errors.Wrap(ErrNotSupported, "filtered search")
	}
	if k < 1 {
		return nil, nil, errors.Errorf("k must be positive, got %d", k)
	}

	p.mu.RLock()
	defer p.mu.RUnlock()
	if err := p.usable(); err != nil {
		return nil, nil, err
	}
	dims := int(p.dims.Load())
	if dims == 0 {
		return nil, nil, nil
	}
	if p.g != nil {
		return p.g.searcher().run(ctx, vector, k, nil, nil)
	}
	if len(vector) != dims {
		return nil, nil, errors.Wrapf(common.ErrWrongDimensions,
			"query has length %d, the index holds vectors of length %d", len(vector), dims)
	}
	if err := checkValues(vector); err != nil {
		return nil, nil, errors.Wrap(err, "query")
	}
	return p.searchStaged(ctx, distancer.Normalize(vector), k)
}

// searchStaged is the search while there is no base: the exact distance to
// every vector kept aside, read from its bucket.
func (p *Persistent) searchStaged(ctx context.Context, query []float32, k int) ([]uint64, []float32, error) {
	var best topK
	best.reset(k)
	vector := make([]float32, len(query))
	seen := 0
	err := growScan(p.staging, func(key, value []byte) error {
		if seen++; seen%1024 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		if len(key) != 4 || len(value) != 4*len(query) {
			return errors.Errorf("a vector kept aside with a key of %d bytes and a value of %d", len(key), len(value))
		}
		for i := range vector {
			vector[i] = getFloat32(value[4*i:])
		}
		dist, err := p.provider.SingleDist(query, vector)
		if err != nil {
			return err
		}
		best.push(uint64(binary.BigEndian.Uint32(key)), dist)
		return nil
	})
	if err != nil {
		return nil, nil, errors.Wrap(err, "search the vectors kept aside")
	}
	return topResults(best.sorted(), k)
}

// SearchByVectorDistance is not supported yet.
func (p *Persistent) SearchByVectorDistance(ctx context.Context, vector []float32, dist float32,
	maxLimit int64, allow helpers.AllowList,
) ([]uint64, []float32, error) {
	return nil, nil, errors.Wrap(ErrNotSupported, "search by distance")
}

// UpdateUserConfig applies a new config. What a search reads on every query
// can change at any time. What shaped what is on disk cannot.
func (p *Persistent) UpdateUserConfig(updated schemaConfig.VectorIndexConfig, callback func()) error {
	defer callback()

	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	if err := ent.ValidateUserConfigUpdate(p.uc, updated); err != nil {
		return err
	}
	parsed := updated.(ent.UserConfig)

	p.mu.Lock()
	defer p.mu.Unlock()
	p.uc = parsed
	if p.g != nil {
		p.g.ix.storeSearchConfig(searchConfig(parsed, scanBitsOf(&p.g.meta)))
	}
	return nil
}

func (p *Persistent) ValidateBeforeInsert(vector []float32) error {
	dims := int(p.dims.Load())
	if dims != 0 && dims != len(vector) {
		return errors.Wrapf(common.ErrWrongDimensions,
			"new vector has length %d, the index holds vectors of length %d", len(vector), dims)
	}
	return checkValues(vector)
}

// ContainsDoc tells whether the index holds a vector under the doc id. A
// deleted one is not held.
func (p *Persistent) ContainsDoc(docID uint64) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.g == nil {
		_, staged := p.staged[docID]
		return staged
	}
	return p.g.holds(docID)
}

func (p *Persistent) Iterate(fn func(docID uint64) bool) {
	p.mu.RLock()
	var ids []uint64
	if p.g == nil {
		ids = make([]uint64, 0, len(p.staged))
		for id := range p.staged {
			ids = append(ids, id)
		}
		slices.Sort(ids)
	} else {
		for id := range p.g.live {
			if p.g.holds(uint64(id)) {
				ids = append(ids, uint64(id))
			}
		}
	}
	p.mu.RUnlock()

	for _, id := range ids {
		if !fn(id) {
			return
		}
	}
}

func (p *Persistent) Compressed() bool {
	return true
}

func (p *Persistent) Multivector() bool {
	return false
}

func (p *Persistent) CompressionStats() compressionhelpers.CompressionStats {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.g == nil {
		return compressionhelpers.UncompressedStats{}
	}
	if scan := p.g.st.scanQuantizer; scan != nil {
		return scan.Stats()
	}
	return p.g.st.quantizer.Stats()
}

// QueryVectorDistancer returns the exact distance from the query to a doc,
// read from where the index keeps its vectors.
func (p *Persistent) QueryVectorDistancer(queryVector []float32) common.QueryVectorDistancer {
	query := distancer.Normalize(queryVector)
	return common.QueryVectorDistancer{
		DistanceFunc: func(docID uint64) (float32, error) {
			p.mu.RLock()
			defer p.mu.RUnlock()
			if err := p.usable(); err != nil {
				return 0, err
			}
			vector, err := p.vectorOf(docID)
			if err != nil {
				return 0, err
			}
			if len(vector) != len(query) {
				return 0, errors.Wrapf(common.ErrWrongDimensions,
					"query has length %d, the index holds vectors of length %d", len(query), len(vector))
			}
			return p.provider.SingleDist(query, vector)
		},
	}
}

// vectorOf reads the vector the index holds under a doc id, as it stores it.
// The caller holds mu.
func (p *Persistent) vectorOf(docID uint64) ([]float32, error) {
	if p.g != nil {
		if !p.g.holds(docID) {
			return nil, storobj.NewErrNotFoundf(docID, "the index does not hold it")
		}
		_, vector, err := p.g.exact(docID, nil, nil)
		return vector, err
	}
	if _, staged := p.staged[docID]; !staged {
		return nil, storobj.NewErrNotFoundf(docID, "the index does not hold it")
	}
	raw, err := p.staging.Get(growDocKey(docID))
	if err != nil {
		return nil, errors.Wrapf(err, "read the vector of doc %d", docID)
	}
	if len(raw) == 0 || len(raw)%4 != 0 {
		return nil, errors.Errorf("the vector of doc %d takes %d bytes", docID, len(raw))
	}
	vector := make([]float32, len(raw)/4)
	for i := range vector {
		vector[i] = getFloat32(raw[4*i:])
	}
	return vector, nil
}

// Preload has nothing to warm: the index keeps no vector in memory.
func (p *Persistent) Preload(id uint64, vector []float32) {}

// PostStartup has nothing left to restore: Open reads what the index keeps in
// memory.
func (p *Persistent) PostStartup(ctx context.Context) {}

// Flush hands to the system what the index has written since the last one:
// the logs of its buckets, the log of the centroid graph and its meta. It
// does not wait for any of it to reach the disk.
func (p *Persistent) Flush() error {
	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	if p.closed || p.failed != nil || !p.dirty {
		return nil
	}
	if p.staging != nil {
		if err := p.staging.WriteWAL(); err != nil {
			return errors.Wrap(err, "write the log of the vectors kept aside")
		}
	}
	if p.g != nil {
		if err := p.g.sync(); err != nil {
			return err
		}
	}
	p.dirty = false
	return nil
}

// ListFiles is of a backup, which the index does not take part in.
func (p *Persistent) ListFiles(ctx context.Context, basePath string) ([]string, error) {
	return nil, errors.Wrap(ErrNotSupported, "backup")
}

// PrepareForBackup fails rather than let a backup complete without the files
// of the index in it.
func (p *Persistent) PrepareForBackup(ctx context.Context) error {
	return errors.Wrap(ErrNotSupported, "backup")
}

// ResumeAfterBackup has nothing to resume, PrepareForBackup never succeeds.
func (p *Persistent) ResumeAfterBackup(ctx context.Context) error {
	return nil
}

// SnapshotMutableFiles fails for the same reason PrepareForBackup does.
func (p *Persistent) SnapshotMutableFiles(ctx context.Context, basePath, stagingDir string) ([]string, error) {
	return nil, errors.Wrap(ErrNotSupported, "backup")
}

// Shutdown writes what the index has not written yet and releases it. Its
// buckets leave the store: the index that opens them next gives them to it
// again.
func (p *Persistent) Shutdown(ctx context.Context) error {
	// What runs in the background ends first: it takes the locks below.
	p.stopMaintenance()
	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.teardown(ctx)
}

// Drop releases the index and, unless keepFiles is set, removes its directory
// and its buckets.
func (p *Persistent) Drop(ctx context.Context, keepFiles bool) error {
	p.stopMaintenance()
	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.teardown(ctx); err != nil || keepFiles {
		return err
	}
	if err := os.RemoveAll(p.dir); err != nil {
		return errors.Wrap(err, "remove the directory of the index")
	}
	for _, name := range helpers.PostingEdgeBucketNames(p.cfg.ID) {
		if err := os.RemoveAll(filepath.Join(p.store.GetDir(), name)); err != nil {
			return errors.Wrapf(err, "remove the bucket %s", name)
		}
	}
	return nil
}

// teardown closes the index. The caller holds writeMu and mu.
func (p *Persistent) teardown(ctx context.Context) error {
	if p.closed {
		return nil
	}
	p.closed = true
	var first error
	keep := func(err error) {
		if first == nil {
			first = err
		}
	}
	if p.g != nil {
		if p.failed != nil {
			// What a write left halfway is not written down as the index:
			// the meta stays as the last flush left it.
			keep(p.g.release(ctx))
		} else {
			keep(p.g.close(ctx))
		}
		p.g = nil
	}
	keep(p.closeStaging(ctx))
	return first
}
