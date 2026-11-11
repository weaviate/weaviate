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

package tachiomfresh

import (
	"context"
	stderrors "errors"
	"math"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/queue"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hfresh"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	ent "github.com/weaviate/weaviate/entities/vectorindex/tachiomfresh"
)

// TachiomFresh is a token-level IVF index for multi-vectors.
// Every token vector is placed in the posting of a centroid. A posting
// holds (docID, tokenPos, version) entries. A search runs TACHIOM's three stages
// on HFresh's centroid HNSW (search.go). Stage 1 searches the centroid
// graph with every query token and sums centroid similarities per
// document. Stage 2 keeps the kDocsToScore documents with the highest sums.
// Stage 3 scores them with the exact MaxSim on their float32 tokens.
// Splits, merges and reassigns run in the background, and documents can be
// deleted, following hfresh. The rules are hfresh's. The posting entry
// differs (posting.go): it stores no vector, so the distances hfresh
// computes on the codes in its postings are computed here on the float32
// tokens read from the object store (centroidDistance in split.go). The
// index is the vector index type "tachiomfresh"
// (entities/vectorindex/tachiomfresh). index.go holds the methods the
// shard's VectorIndex interface asks for beyond insert, delete and search.
//
// Every document comes with the token id of each token, the id the
// tokenizer of the model gave it, and a document without them is refused
// (insert.go). The token id is the token's type. The index keeps the
// centroids of each type apart: a token is placed, split, merged and
// reassigned among the centroids of its type only, so every posting holds
// one type (types.go). A token goes to the posting of the centroid of its
// type that is nearest in L2, the distance the split clusters by. The
// centroid HNSW is searched by queries alone: a query token searches all
// centroids with the index distance, the dot product (nearestCentroids in
// types.go; hnsw.go says why the two distances differ).
type TachiomFresh struct {
	id        string
	logger    logrus.FieldLogger
	config    *hfresh.Config
	metrics   *hfresh.Metrics
	scheduler *queue.Scheduler
	store     *lsmkv.Store

	// l2 is the squared L2 distance. The split, merge and reassign checks
	// compare it (centroidDistance), and the placement of a
	// token ranks the centroids by it (nearestCentroids; see hnsw.go).
	l2 distancer.Provider

	// The configured posting limits, read through postingLimits (types.go),
	// where they are the limits of the small token types and the floor of
	// the others.
	maxPostingSize uint32 // tokens per posting before a split
	minPostingSize uint32 // tokens per posting below which it is merged
	// The search budgets. The search reads them atomically, and
	// UpdateUserConfig sets them on a running index, like hfresh's
	// searchProbe and rescoreLimit (setSearchBudgets).
	kCentroids   uint32 // centroids searched per query token (stage 1)
	kDocsToScore uint32 // candidates kept for the final MaxSim (stage 2)

	// opts holds what only the tests change; New sets the production values.
	opts testOptions

	// dims is published once by initDimensions, the shape of
	// hfresh.initDimensions without the quantizer: the index compresses
	// nothing.
	initMu   sync.Mutex
	initDone bool
	dims     uint32

	// center is the token mean that every document token is centered on
	// before it is compared with a centroid (centerVec). nil means no
	// centering. The index computes the mean itself when it is built, from
	// every token staged under the threshold (buildFromStaging). The mean
	// is persisted in the type bucket, restored by restore, and never
	// changed by a later insert or delete. What is centered, why a query
	// token is not, and why this needs the dot distance: see the Centering
	// paragraph at the top of staging.go.
	center []float32

	// The staging phase (staging.go). centerTokens is the threshold: the
	// index is built when the staged tokens reach it. built says the index
	// has passed the threshold. It is also true for an index that never
	// staged (centerTokens 0) and for a restored built index. stageMu
	// guards the staging bucket, stagedDocs (tokens per staged doc id),
	// stagedTokens and the build. An insert, a delete or a search of a
	// built index never takes it.
	centerTokens uint64
	built        atomic.Bool
	stageMu      sync.Mutex
	staging      *stagingStore
	stagedDocs   map[uint64]uint32
	stagedTokens uint64

	// Internal components
	Centroids     *HNSWIndex                 // centroid id equals posting id
	PostingStore  *postingStore              // persists the postings
	IDs           *common.Sequence           // posting ids, as HFresh.IDs
	VersionMap    *hfresh.VersionMap         // delete tombstones per document
	IndexMetadata *hfresh.IndexMetadataStore // persists the dimensions
	postings      *postingTable              // in-memory postings
	types         *typeTable                 // posting types and the centroids of each type
	docs          *docSet                    // the documents indexed and not deleted (docs.go)
	// placed holds the documents the index has placed tokens of, whole or
	// in part, deleted or not. A new insert of such an id is a re-insert,
	// which raises its version so that its earlier entries become stale
	// (fileDocument). counts holds each placed document's token count.
	placed *docSet
	counts *tokenCountStore

	// ctx and cancel are used to manage the lifecycle of the background operations.
	ctx    context.Context
	cancel context.CancelFunc

	taskQueue TaskQueue

	postingLocks       *common.ShardedRWLocks // as HFresh.postingLocks
	initialPostingLock sync.Mutex             // as HFresh.initialPostingLock

	multiVectorForID common.VectorForID[[]float32]
	// tokenForID, when set, serves the single tokens that splits, merges,
	// reassigns and the neighbour check read, without the document. nil
	// falls back to multiVectorForID.
	// tokenIDsForID reads the token ids stored with a document's object
	// (AddMulti).
	tokenIDsForID common.TokenIDsForID
	tokenForID    common.TokenForID
	// tempMultiVectorForID, when set, serves the candidates of stage 3 into
	// the query's reused container, so no fresh memory is allocated per
	// candidate. nil falls back to multiVectorForID.
	tempMultiVectorForID common.TempVectorForID[[]float32]
}

// testOptions is what the tests change or observe in an index. Production
// never changes it from what New sets.
type testOptions struct {
	// The two mechanisms of a filtered search (SearchByMultiVector,
	// search.go), so that the tests can measure each one alone. flatFactor is
	// flatSearchFactor (0 disables the flat path). maxWiden is maxWidenings
	// (0 disables the widening).
	flatFactor int
	maxWiden   int
	// disableReassign, like hfresh's disableIntermediateRescore: splits and
	// merges enqueue no reassign.
	disableReassign bool
	// onWiden, when set, is called each time a filtered search doubles
	// kCentroids.
	onWiden func()
	// onCentered, when set, is called with the number of tokens a build took
	// its mean from.
	onCentered func(tokens uint64)
	// beforeAppend, when set, is called with the target posting before an
	// insert appends a token to it or a reassign moves one into it, with no
	// lock held.
	beforeAppend func(postingID uint64)
	// beforeBuiltWAL, when set, is called before the build writes out the
	// built phase; an error it returns fails that write.
	beforeBuiltWAL func() error
	// beforeCentroidDelete, when set, is called before a split or a merge
	// deletes a centroid; an error it returns fails the delete.
	beforeCentroidDelete func(postingID uint64) error
	// beforeCandidateLock, when set, is called before a merge takes the lock
	// of a candidate on another lock stripe, with the merged posting's lock
	// held.
	beforeCandidateLock func(candidateID uint64)
}

// copied from hfresh's config.go (unexported there)
const minPostingSizeFloor uint32 = 3

// New builds an index on cfg, or reopens the one that cfg and store already
// hold, in the shape of hfresh.New. From the user config it takes
// maxPostingSize (the split threshold in tokens), kCentroids and
// kDocsToScore (TACHIOM's search budgets), and efConstruction
// (the centroid HNSW's; hfresh uses 64, see hnsw.go). The distance comes
// from cfg.DistanceProvider, which the shard builds from the user config's
// distance, as it does for hfresh.
func New(cfg *hfresh.Config, uc ent.UserConfig, store *lsmkv.Store) (*TachiomFresh, error) {
	maxPostingSize, kCentroids, kDocsToScore, efConstruction := uc.MaxPostingSize, uc.KCentroids, uc.KDocsToScore, uc.EFConstruction

	// cfg.Validate is not called: it demands the single-vector thunks,
	// which a token-level index does not use. Its defaults are set by hand
	// here. ReassignNeighbors is left as given, so 0 turns the neighbour
	// check off, as enqueueReassignAfterSplit reads it.
	cfg.Logger = common.LoggerOrDiscard(cfg.Logger)
	if cfg.InternalPostingCandidates <= 0 {
		cfg.InternalPostingCandidates = hfresh.DefaultInternalPostingCandidates
	}

	// The metric of a multi-vector index is MaxSim over the dot product.
	// Stage 1 sums centroid similarities, and a document no query token
	// reached scores 0, as in TACHIOM: -dist of the dot distance is the
	// similarity. L2 has no such similarity: -dist is negative for every
	// match, so every document the search reached would rank below the ones
	// it did not reach. Centering needs the dot distance too (staging.go).
	if got := cfg.DistanceProvider.Type(); got != "dot" {
		return nil, errors.Errorf("tachiomfresh supports the dot distance only, got %q", got)
	}
	// splits, merges, reassigns and stage 3 read document tokens through the
	// thunk. Nothing else holds the full precision.
	if cfg.MultiVectorForIDThunk == nil {
		return nil, errors.New("multiVectorForIDThunk cannot be nil")
	}
	// split, merge and reassign run on the scheduler, as in hfresh
	if cfg.Scheduler == nil {
		return nil, errors.New("scheduler cannot be nil")
	}
	if maxPostingSize == 0 || kCentroids == 0 || kDocsToScore == 0 || efConstruction <= 0 {
		return nil, errors.New("maxPostingSize, kCentroids, kDocsToScore and efConstruction must be positive")
	}
	// copied from hfresh.setMaxPostingSize
	minPostingSize := max(maxPostingSize/3, minPostingSizeFloor)
	if minPostingSize >= maxPostingSize {
		return nil, errors.New("min posting size must be less than max posting size")
	}

	metrics := hfresh.NewMetrics(cfg.PrometheusMetrics, cfg.ClassName, cfg.ShardName)

	// initialize shared bucket used for storing various metadata
	bucket, err := hfresh.NewSharedBucket(store, cfg.ID, cfg.Store)
	if err != nil {
		return nil, err
	}

	// initialize posting store used for persisting the postings
	postingStore, err := newPostingStore(store, metrics, cfg.ID, cfg.Store)
	if err != nil {
		return nil, err
	}

	// initialize the bucket that persists the type of each posting
	types, err := newTypeTable(store, cfg.ID, cfg.Store)
	if err != nil {
		return nil, err
	}

	// initialize the bucket that holds the documents staged under the
	// threshold (staging.go)
	staging, err := newStagingStore(store, cfg.ID, cfg.Store)
	if err != nil {
		return nil, err
	}

	// initialize the bucket that holds each document's token count
	// (tokencount.go)
	counts, err := newTokenCountStore(store, cfg.ID, cfg.Store)
	if err != nil {
		return nil, err
	}

	t := TachiomFresh{
		id:                   cfg.ID,
		logger:               cfg.Logger.WithField("component", "TachiomFresh"),
		config:               cfg,
		metrics:              metrics,
		scheduler:            cfg.Scheduler,
		store:                store,
		l2:                   distancer.NewL2SquaredProvider(),
		maxPostingSize:       maxPostingSize,
		minPostingSize:       minPostingSize,
		kCentroids:           kCentroids,
		kDocsToScore:         kDocsToScore,
		opts:                 testOptions{flatFactor: flatSearchFactor, maxWiden: maxWidenings},
		PostingStore:         postingStore,
		VersionMap:           hfresh.NewVersionMap(bucket),
		IndexMetadata:        hfresh.NewIndexMetadataStore(bucket),
		postings:             &postingTable{},
		types:                types,
		docs:                 &docSet{},
		placed:               &docSet{},
		counts:               counts,
		centerTokens:         defaultCenterTokens,
		staging:              staging,
		stagedDocs:           map[uint64]uint32{},
		postingLocks:         common.NewDefaultShardedRWLocks(),
		multiVectorForID:     cfg.MultiVectorForIDThunk,
		tokenForID:           cfg.TokenForIDThunk,
		tokenIDsForID:        cfg.TokenIDsForIDThunk,
		tempMultiVectorForID: cfg.TempMultiVectorForIDThunk,
	}

	t.Centroids, err = NewHNSWIndex(metrics, store, cfg, 1024*1024, 1024, efConstruction)
	if err != nil {
		return nil, err
	}
	// The persisted bound of the id sequence is at least every posting id
	// handed out before this open. restore reads it to find the postings
	// left without a centroid.
	idStore := hfresh.NewBucketStore(bucket)
	idBound, err := idStore.Load()
	if err != nil {
		t.releaseAfterFailedNew(nil)
		return nil, errors.Wrap(err, "failed to load the posting id sequence")
	}
	t.IDs, err = common.NewSequence(idStore, 1000)
	if err != nil {
		t.releaseAfterFailedNew(nil)
		return nil, err
	}

	t.ctx, t.cancel = context.WithCancel(context.Background())

	taskQueue, err := NewTaskQueue(&t)
	if err != nil {
		t.cancel()
		t.releaseAfterFailedNew(nil)
		return nil, err
	}
	t.taskQueue = *taskQueue

	if err = t.restore(idBound); err != nil {
		t.cancel()
		t.releaseAfterFailedNew(&t.taskQueue)
		return nil, errors.Wrapf(err, "unable to restore metadata from previous run")
	}

	t.taskQueue.Register()

	return &t, nil
}

// releaseAfterFailedNew stops what New started before it failed: the task
// queue, if it was created, and the centroid HNSW. The caller gets New's
// error, so a failure here is only logged.
func (t *TachiomFresh) releaseAfterFailedNew(taskQueue *TaskQueue) {
	ctx := context.Background()
	if taskQueue != nil {
		if err := taskQueue.Close(ctx); err != nil {
			t.logger.Warnf("failed to close the task queue after a failed start: %v", err)
		}
	}
	if err := t.Centroids.hnsw.Shutdown(ctx); err != nil {
		t.logger.Warnf("failed to shut down the centroid index after a failed start: %v", err)
	}
}

// restore reloads what a previous run persisted: the dimensions, and the
// posting table from the posting store. It is the counterpart of
// hfresh.restoreMetadata, which reloads the posting map and the sizes.
// Here the postings themselves are the in-memory state. A posting exists
// exactly when its centroid does (centroid id equals posting id), so the
// centroid HNSW, already replayed from its commit log, names the postings
// to load. The type of every posting and the groups come back too. The
// entries of each type are recounted from the postings. An index saved in
// another posting format is refused (see checkPostingFormat). The doc set
// (docs.go) is rebuilt from the doc ids of the postings minus the deleted
// ones. For that check the version map is warmed first, in the foreground, by
// hfresh's warmVersionMap, so that the check reads memory. A document
// with no persisted version is given its implicit first version, as
// hfresh does, which saves one store read per document. idBound is the
// persisted bound of the posting id sequence: no posting id handed out
// before this open is above it.
func (t *TachiomFresh) restore(idBound uint64) error {
	dims, err := t.IndexMetadata.GetDimensions()
	if err != nil || dims == 0 {
		// nothing inserted yet: the index stages its first documents
		// unless the threshold is 0
		t.built.Store(t.centerTokens == 0)
		return err
	}

	if err := t.types.checkPostingFormat(); err != nil {
		return err
	}

	t.initMu.Lock()
	t.initDone = true
	atomic.StoreUint32(&t.dims, dims)
	t.initMu.Unlock()

	if _, err := t.VersionMap.Warmup(t.ctx); err != nil {
		return errors.Wrap(err, "failed to warm the version map")
	}

	// An index saved under the threshold holds its documents in the staging
	// bucket and no posting. An index that has centroids while its phase is
	// still staging was interrupted while being built: what the build placed
	// is dropped, and the staging bucket still holds every document (see
	// buildFromStaging).
	phase, err := t.types.getPhase()
	if err != nil {
		return errors.Wrap(err, "failed to restore the index phase")
	}
	if phase == phaseUnset {
		phase = phaseBuilt
		if t.centerTokens > 0 && !t.hasCentroids() {
			phase = phaseStaging
			if err := t.types.setPhase(phaseStaging); err != nil {
				return errors.Wrap(err, "could not persist the index phase")
			}
		}
	}
	if phase == phaseStaging {
		if err := t.resetBuild(); err != nil {
			return errors.Wrap(err, "failed to reset an interrupted build")
		}
		t.built.Store(false)
		return t.restoreStaging()
	}
	t.built.Store(true)
	if err := t.dropLeftoverStaged(); err != nil {
		return errors.Wrap(err, "failed to drop the staged documents of a built index")
	}

	if err := t.types.restore(t.Centroids.Exists); err != nil {
		return errors.Wrap(err, "failed to restore the posting types")
	}

	center, err := t.types.getCenter()
	if err != nil {
		return errors.Wrap(err, "failed to restore the centering mean")
	}
	if center != nil && len(center) != int(dims) {
		return errors.Errorf("the saved centering mean has %d dimensions, the index %d", len(center), dims)
	}
	t.center = center

	docs, err := t.restoreDocCounts()
	if err != nil {
		return err
	}
	var last uint64
	t.Centroids.hnsw.Iterate(func(id uint64) bool {
		last = max(last, id)
		if !t.Centroids.Exists(id) {
			return true
		}
		var refs []tokenRef
		refs, err = t.PostingStore.Get(id)
		if err != nil {
			err = errors.Wrapf(err, "failed to restore posting %d", id)
			return false
		}
		t.postings.set(id, refs)
		// the entry count of each type is not persisted
		t.countEntries(id, len(refs))
		for _, ref := range refs {
			if err = docs.see(t, ref); err != nil {
				return false
			}
		}
		return true
	})
	if err != nil {
		return err
	}
	if err := t.resolveDuplicates(docs.duplicates); err != nil {
		return err
	}
	if err := docs.fill(t); err != nil {
		return err
	}
	// The variances and their reference are computed now that every type
	// has its entry count. The sums came back with the types.
	t.types.refreshVariances()
	// The set of dropped postings is not persisted, and their keys may still
	// be on disk. Nothing is in flight at this point, and ids are never
	// reused. So an id handed out before this open that has no centroid is
	// a posting a split or a merge gave up, or one a split stored before a
	// crash came ahead of its centroid. Such an id can be above the last
	// live centroid's id, up to the bound of the id sequence.
	for id := range max(last, idBound) + 1 {
		if !t.Centroids.Exists(id) {
			t.PostingStore.Drop(id)
		}
	}
	return nil
}

// restoredDocs is what restore learns about the documents from the token
// counts and the postings. A document is indexed when its live entries
// cover every position its stored count names. Positions are marked in one
// bitset, a document's range at its offset, so that a token found twice is
// seen as a duplicate and not counted twice.
type restoredDocs struct {
	index      map[uint32]int // doc id to its entry in docs
	docs       []restoredDoc
	bits       []uint64
	duplicates map[tokenRef]struct{}
}

type restoredDoc struct {
	id     uint32
	tokens int // the stored count
	seen   int // positions with a live entry
	offset int // of the document's first position in bits
}

// restoreDocCounts reads the token counts. Every document with a count has
// had tokens placed (placed).
func (t *TachiomFresh) restoreDocCounts() (*restoredDocs, error) {
	r := &restoredDocs{index: map[uint32]int{}, duplicates: map[tokenRef]struct{}{}}
	var total int
	err := t.counts.iterate(func(docID uint64, tokens int) error {
		t.placed.add(docID)
		r.index[uint32(docID)] = len(r.docs)
		r.docs = append(r.docs, restoredDoc{id: uint32(docID), tokens: tokens, offset: total})
		total += tokens
		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to read the token counts")
	}
	r.bits = make([]uint64, (total+63)/64)
	return r, nil
}

// see counts entry ref of a restored posting. A live entry marks its
// position; a position marked already is a token found in two postings,
// which a crash during a move or a split can leave (resolveDuplicates).
func (r *restoredDocs) see(t *TachiomFresh, ref tokenRef) error {
	t.placed.add(uint64(ref.DocID))
	// a document the version store does not hold was never deleted or
	// re-inserted: the store sees only those (VersionMap.EnsureDefault)
	t.VersionMap.EnsureDefault(uint64(ref.DocID))
	live, err := t.live(t.ctx, ref)
	if err != nil || !live {
		return err
	}
	i, ok := r.index[ref.DocID]
	if !ok || int(ref.Pos) >= r.docs[i].tokens {
		return nil
	}
	doc := &r.docs[i]
	bit := doc.offset + int(ref.Pos)
	if r.bits[bit/64]&(1<<(bit%64)) != 0 {
		r.duplicates[ref] = struct{}{}
		return nil
	}
	r.bits[bit/64] |= 1 << (bit % 64)
	doc.seen++
	return nil
}

// fill adds to the doc set every document whose live entries cover its
// stored count and that is not deleted. A document without a stored count,
// or with fewer live entries, was placed in part: it stays out, so the
// shard sends it again.
func (r *restoredDocs) fill(t *TachiomFresh) error {
	for _, doc := range r.docs {
		if doc.seen != doc.tokens {
			continue
		}
		version, err := t.VersionMap.Get(t.ctx, uint64(doc.id))
		if err != nil {
			return errors.Wrapf(err, "failed to restore the version of doc %d", doc.id)
		}
		if !version.Deleted() {
			t.docs.add(uint64(doc.id))
		}
	}
	return nil
}

// resolveDuplicates keeps one copy of each token found in more than one
// posting: the copy in the posting whose centroid is nearest the token, by
// the rule that places a token (placementDistance), which is the rule the
// interrupted move followed. The other copies are removed, in memory and on
// disk. A token that cannot be read keeps the copy found first.
func (t *TachiomFresh) resolveDuplicates(duplicates map[tokenRef]struct{}) error {
	if len(duplicates) == 0 {
		return nil
	}
	where := make(map[tokenRef][]uint64, len(duplicates))
	t.postings.mu.RLock()
	lists := t.postings.lists
	t.postings.mu.RUnlock()
	for id, p := range lists {
		if p.len() == 0 {
			continue
		}
		for _, ref := range p.decode() {
			if _, ok := duplicates[ref]; ok {
				where[ref] = append(where[ref], uint64(id))
			}
		}
	}
	for ref, ids := range where {
		keep := ids[0]
		token, err := t.fetchToken(t.ctx, ref)
		if err == nil {
			best := float32(math.MaxFloat32)
			for _, id := range ids {
				dist, err := t.placementDistance(token, id)
				if err != nil {
					return err
				}
				if dist < best {
					keep, best = id, dist
				}
			}
		} else {
			t.logger.WithField("docID", ref.DocID).
				Warnf("token %d found in %d postings and not readable, keeping posting %d: %v", ref.Pos, len(ids), keep, err)
		}
		for _, id := range ids {
			if id == keep {
				continue
			}
			if err := t.PostingStore.Remove(id, ref); err != nil {
				return errors.Wrapf(err, "failed to remove a duplicate of token %+v from posting %d", ref, id)
			}
			t.postings.remove(id, ref)
			t.countEntries(id, -1)
		}
	}
	return nil
}

// copied from HFresh.Shutdown
func (t *TachiomFresh) Shutdown(ctx context.Context) error {
	if t.ctx == nil {
		return nil // Already closed or not started
	}

	if t.ctx.Err() != nil {
		return t.ctx.Err() // Context already cancelled
	}

	// Cancel the context to prevent new operations from being enqueued
	t.cancel()

	var errs []error

	err := t.taskQueue.Close(ctx)
	if err != nil {
		errs = append(errs, err)
	}

	err = t.Flush()
	if err != nil {
		errs = append(errs, err)
	}

	err = t.IDs.Flush()
	if err != nil {
		errs = append(errs, err)
	}

	err = t.Centroids.hnsw.Shutdown(ctx)
	if err != nil {
		errs = append(errs, err)
	}

	return stderrors.Join(errs...)
}

// copied from HFresh.Flush
func (t *TachiomFresh) Flush() error {
	var errs []error

	// not hfresh's: persist the spread sums of the token types
	// (persistSpread, types.go). They go into the type bucket before its
	// write buffer is written out below, so that they are on disk when
	// Flush returns.
	err := t.types.persistSpread()
	if err != nil {
		errs = append(errs, err)
	}

	// not hfresh's: the buckets' write buffers go to disk before the
	// commit log's (see writeWALs)
	err = t.writeWALs()
	if err != nil {
		errs = append(errs, err)
	}

	// flush the HNSW commit log
	err = t.Centroids.hnsw.Flush()
	if err != nil {
		errs = append(errs, err)
	}

	// flush the task queues
	err = t.taskQueue.Flush()
	if err != nil {
		errs = append(errs, err)
	}

	return stderrors.Join(errs...)
}

// hasCentroids reports whether the centroid HNSW holds a live centroid.
func (t *TachiomFresh) hasCentroids() bool {
	found := false
	t.Centroids.hnsw.Iterate(func(id uint64) bool {
		found = t.Centroids.Exists(id)
		return !found
	})
	return found
}

// writeWALs writes out the write buffers of the buckets a centroid change
// depends on: the postings, the posting types, the shared bucket that holds
// the posting id sequence and the dimensions, and the token counts that
// restore checks the postings against (restoredDocs). Each bucket buffers
// its write-ahead log until the buffer fills or WriteWAL is called, and the
// centroid HNSW's commit log has its own buffer. Called before a centroid is
// inserted or deleted, it makes the postings, types and ids that the change
// relies on reach their log files first. WriteWAL does not fsync: the order
// holds for a crash of the process, not for a crash of the machine, where
// the files can reach the storage in either order. A crash after the
// change then never reopens a centroid without its posting.
//
// The other order is not kept, as in hfresh, which may have the same
// problem: the centroid HNSW's commit log is written out by Flush only, so
// a centroid change can reach its file after the postings that depend on
// it. A split drops the old posting (dropPosting) while the old centroid's
// delete may still be in the commit log's buffer, and a reassign can move a
// token into a new centroid whose insert is still there. A crash before the
// commit log is written can then reopen the old centroid with its entries
// gone from disk, or lose the new centroid with the moved token in its
// posting. Writing the commit log out at every centroid change would close
// this, at the cost of one more write per split and merge.
func (t *TachiomFresh) writeWALs() error {
	for _, name := range []string{
		helpers.TachiomFreshPostingsBucketName(t.id),
		helpers.TachiomFreshTypesBucketName(t.id),
		helpers.HFreshSharedBucketName(t.id),
		helpers.TachiomFreshTokenCountsBucketName(t.id),
	} {
		bucket, release := t.store.AcquireBucketForRead(name)
		if bucket == nil {
			release()
			return errors.Wrapf(lsmkv.ErrBucketNotFound, "bucket %s", name)
		}
		err := bucket.WriteWAL()
		release()
		if err != nil {
			return errors.Wrapf(err, "failed to write the WAL of bucket %s", name)
		}
	}
	return nil
}

// initDimensions publishes the token dimensionality on the first insert and
// persists it for restore. copied from hfresh.initDimensions, without the
// quantizer, distancer and posting size setup.
func (t *TachiomFresh) initDimensions(vector []float32) error {
	t.initMu.Lock()
	defer t.initMu.Unlock()

	if t.initDone {
		// ValidateMultiBeforeInsert ran before the dimensions were set by a
		// concurrent first insert
		if dims := atomic.LoadUint32(&t.dims); uint32(len(vector)) != dims {
			return errors.Errorf("new node has a multi-vector with dimension %d. Existing nodes have dimensions %d", len(vector), dims)
		}
		return nil
	}

	if err := t.types.setPostingFormat(); err != nil {
		return errors.Wrap(err, "could not persist the posting format")
	}
	// the phase is written before the dimensions: an index restore finds with
	// dimensions has its phase
	phase := phaseStaging
	if t.centerTokens == 0 {
		phase = phaseBuilt
	}
	if err := t.types.setPhase(phase); err != nil {
		return errors.Wrap(err, "could not persist the index phase")
	}

	size := uint32(len(vector))
	if err := t.IndexMetadata.SetDimensions(size); err != nil {
		return errors.Wrap(err, "could not persist dimensions")
	}
	// The format, the phase and the dimensions are written out before the
	// first document: a staged document can be larger than a bucket's
	// write buffer and reach the disk at once, and restore reads the
	// staging bucket only when it finds the dimensions.
	if err := t.writeWALs(); err != nil {
		return err
	}
	t.initDone = true
	atomic.StoreUint32(&t.dims, size)
	return nil
}

// centerVec returns vec minus the centering mean, in a new slice, or vec
// itself when the index centers nothing. It is called on every document
// token before the token is compared with a centroid, and never on a query
// token (see center).
func (t *TachiomFresh) centerVec(vec []float32) []float32 {
	if t.center == nil {
		return vec
	}
	out := make([]float32, len(vec))
	for i, v := range vec {
		out[i] = v - t.center[i]
	}
	return out
}

// centerMultiVec applies centerVec to every token of a document. The
// document's own slices are not changed.
func (t *TachiomFresh) centerMultiVec(vecs [][]float32) [][]float32 {
	if t.center == nil {
		return vecs
	}
	centered := make([][]float32, len(vecs))
	for i, vec := range vecs {
		centered[i] = t.centerVec(vec)
	}
	return centered
}

// similarity turns a centroid HNSW distance into the centroid similarity
// that stage 1 accumulates (TACHIOM sums raw dot products): -dist, as the dot
// distance is -dot.
func (t *TachiomFresh) similarity(dist float32) float32 {
	return -dist
}
