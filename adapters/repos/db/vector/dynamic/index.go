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

package dynamic

import (
	"context"
	"encoding/binary"
	simpleErrors "errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/shardmeta"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/flat"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	schemaconfig "github.com/weaviate/weaviate/entities/schema/config"
	ent "github.com/weaviate/weaviate/entities/vectorindex/dynamic"
	"github.com/weaviate/weaviate/usecases/byteops"
	"github.com/weaviate/weaviate/usecases/memwatch"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

const (
	composerUpgradedKey = "upgraded"
	batchSize           = 500

	// upgradingMarkerKey is the base key, in its OWN namespace disjoint from the
	// verdict key ("upgraded"), recording an in-progress flat→HNSW upgrade. It is
	// written durably before the upgrade touches any bucket and cleared only after
	// a durable repair or a committed upgrade; its presence on load means the
	// upgrade was interrupted, so the flat stage rebuilds the shared compressed
	// bucket a quantized upgrade may have polluted with HNSW-format codes.
	//
	// It must NOT be derived by suffixing the verdict key: "upgraded_<tv>" +
	// "_upgrading" would collide with the verdict key of a target vector literally
	// named "<tv>_upgrading". Keying off a distinct prefix ("upgrading_<tv>" vs
	// "upgraded_<tv>") keeps the two keyspaces disjoint for every target-vector
	// name, since "upgraded…" and "upgrading…" diverge before any suffix.
	//
	// A separate key rather than a third verdict value is also deliberate: the
	// verdict keeps its original two states and old semantics, so an OLD binary
	// that ignores this key reads verdict {0} and boots the flat stage against the
	// polluted bucket — failing loud with "vector lengths don't match" (the
	// original bug, no worse) rather than silently booting a half-populated HNSW.
	// See https://github.com/weaviate/0-weaviate-issues/issues/451.
	upgradingMarkerKey = "upgrading"
)

// Verdict bytes stored under the dynamic index's state key. The stage a shard
// boots into is decided by this byte (see init/UpgradedOnDisk). An in-progress
// upgrade is tracked out of band, under upgradingMarkerKey's own namespace.
//
//   - verdictFlat: the index is (still) the flat stage.
//   - verdictUpgraded: the flat→HNSW upgrade committed; boot HNSW.
const (
	verdictFlat     byte = 0
	verdictUpgraded byte = 1
)

// StateNamespace is the metadata-DB namespace holding the dynamic index's
// per-target-vector flat-to-hnsw state. The shard scopes the bounded
// operations it hands to Config.State to this namespace.
const StateNamespace = "dynamic"

// dynamicBucket is referenced only by test files that seed or assert the
// on-disk format through raw bolt; production code goes through
// StateNamespace-scoped state ops.
var dynamicBucket = []byte(StateNamespace)

type Index interface {
	// UnderlyingIndex returns the underlying index type (flat or hnsw)
	UnderlyingIndex() common.IndexType
	IsUpgraded() bool
}

type VectorIndex interface {
	Add(ctx context.Context, id uint64, vector []float32) error
	AddBatch(ctx context.Context, id []uint64, vector [][]float32) error
	Delete(id ...uint64) error
	SearchByVector(ctx context.Context, vector []float32, k int, allow helpers.AllowList) ([]uint64, []float32, error)
	SearchByVectorDistance(ctx context.Context, vector []float32, dist float32,
		maxLimit int64, allow helpers.AllowList) ([]uint64, []float32, error)
	UpdateUserConfig(updated schemaconfig.VectorIndexConfig, callback func()) error
	Drop(ctx context.Context, keepFiles bool) error
	Shutdown(ctx context.Context) error
	Flush() error
	PrepareForBackup(ctx context.Context) error
	ResumeAfterBackup(ctx context.Context) error
	ListFiles(ctx context.Context, basePath string) ([]string, error)
	SnapshotMutableFiles(ctx context.Context, basePath, stagingDir string) ([]string, error)
	PostStartup(ctx context.Context)
	Compressed() bool
	Multivector() bool
	ValidateBeforeInsert(vector []float32) error
	ContainsDoc(docID uint64) bool
	Preload(id uint64, vector []float32)
	QueryVectorDistancer(queryVector []float32) common.QueryVectorDistancer
	// Iterate over all indexed document ids in the index.
	// Consistency or order is not guaranteed, as the index may be concurrently modified.
	// If the callback returns false, the iteration will stop.
	Iterate(fn func(docID uint64) bool)
	Type() common.IndexType
}

type upgradableIndexer interface {
	Upgraded() bool
	Upgrade(callback func()) error
	ShouldUpgrade() (bool, int)
	AlreadyIndexed() uint64
	UpgradeInProgress() bool
}

var (
	upgrading = "upgrading"
	upgraded  = "upgraded"
)

type status atomic.Pointer[string]

// IsUpgraded returns true if the index has been upgraded from flat to HNSW.
func (s *status) IsUpgraded() bool {
	if s == nil {
		return false
	}
	v := (*atomic.Pointer[string])(s).Load()
	return v != nil && *v == upgraded
}

// IsUpgrading returns true if the index is currently being upgraded from flat to HNSW.
func (s *status) IsUpgrading() bool {
	if s == nil {
		return false
	}
	v := (*atomic.Pointer[string])(s).Load()
	return v != nil && *v == upgrading
}

// Reset sets the status to nil. This is used to indicate that the index is neither upgraded nor upgrading.
func (s *status) Reset() {
	if s == nil {
		return
	}
	(*atomic.Pointer[string])(s).Store(nil)
}

// Upgraded sets the status to upgraded. This is used to indicate that the index has been upgraded from flat to HNSW.
func (s *status) Upgraded() {
	if s == nil {
		return
	}
	(*atomic.Pointer[string])(s).Store(&upgraded)
}

// TryUpgrading claims the upgrade attempt; only the winning caller gets true,
// so a Reset after a failed attempt lets a later call retry.
func (s *status) TryUpgrading() bool {
	if s == nil {
		return false
	}
	return (*atomic.Pointer[string])(s).CompareAndSwap(nil, &upgrading)
}

type dynamic struct {
	sync.RWMutex
	id                           string
	store                        *lsmkv.Store
	logger                       logrus.FieldLogger
	rootPath                     string
	shardName                    string
	className                    string
	prometheusMetrics            *monitoring.PrometheusMetrics
	vectorForIDThunk             common.VectorForID[float32]
	vectorFromObject             hnsw.VectorFromObject
	getViewThunk                 common.GetViewThunk
	tempVectorForIDWithViewThunk common.TempVectorForIDWithView[float32]
	distanceProvider             distancer.Provider
	makeCommitLoggerThunk        hnsw.MakeCommitLogger
	threshold                    uint64
	index                        VectorIndex
	status                       status
	tombstoneCallbacks           cyclemanager.CycleCallbackGroup
	uc                           ent.UserConfig
	state                        StateOps
	ctx                          context.Context
	cancel                       context.CancelFunc
	hnswWaitForCachePrefill      bool
	AllocChecker                 memwatch.AllocChecker
	MakeBucketOptions            lsmkv.MakeBucketOptions
	AsyncIndexingEnabled         bool

	// upgradeFn performs the flat→HNSW rebuild. New wires it to the doUpgrade
	// method; keeping it as an injected field (like the *Thunk dependencies
	// above) lets tests substitute a failing or blocking rebuild without a
	// test-only branch in the production path. Production always runs doUpgrade.
	upgradeFn func() error
}

func New(cfg Config, uc ent.UserConfig, store *lsmkv.Store) (*dynamic, error) {
	if err := cfg.Validate(); err != nil {
		return nil, errors.Wrap(err, "invalid config")
	}

	// in place rather than into a local: the flat config below passes cfg.Logger
	// on, so a default kept beside it would not travel with the index
	cfg.Logger = common.LoggerOrDiscard(cfg.Logger)

	flatConfig := flat.Config{
		ID:                cfg.ID,
		RootPath:          cfg.RootPath,
		Logger:            cfg.Logger,
		DistanceProvider:  cfg.DistanceProvider,
		AllocChecker:      cfg.AllocChecker,
		MakeBucketOptions: cfg.MakeBucketOptions,
	}

	ctx, cancel := context.WithCancel(context.Background())

	index := &dynamic{
		id:                           cfg.ID,
		logger:                       cfg.Logger,
		rootPath:                     cfg.RootPath,
		shardName:                    cfg.ShardName,
		className:                    cfg.ClassName,
		prometheusMetrics:            cfg.PrometheusMetrics,
		vectorForIDThunk:             cfg.VectorForIDThunk,
		vectorFromObject:             cfg.VectorFromObject,
		getViewThunk:                 cfg.GetViewThunk,
		tempVectorForIDWithViewThunk: cfg.TempVectorForIDWithViewThunk,
		distanceProvider:             cfg.DistanceProvider,
		makeCommitLoggerThunk:        cfg.MakeCommitLoggerThunk,
		store:                        store,
		threshold:                    uc.Threshold,
		tombstoneCallbacks:           cfg.TombstoneCallbacks,
		uc:                           uc,
		state:                        cfg.State,
		ctx:                          ctx,
		cancel:                       cancel,
		hnswWaitForCachePrefill:      cfg.HNSWWaitForCachePrefill,
		AllocChecker:                 cfg.AllocChecker,
		MakeBucketOptions:            cfg.MakeBucketOptions,
		AsyncIndexingEnabled:         cfg.AsyncIndexingEnabled,
	}
	index.upgradeFn = index.doUpgrade

	upgraded, interrupted, err := index.init(&cfg)
	if err != nil {
		return nil, err
	}

	if upgraded {
		index.status.Upgraded()
		hnsw, err := hnsw.New(
			hnsw.Config{
				Logger:                       index.logger,
				RootPath:                     index.rootPath,
				ID:                           index.id,
				ShardName:                    index.shardName,
				ClassName:                    index.className,
				PrometheusMetrics:            index.prometheusMetrics,
				VectorForIDThunk:             index.vectorForIDThunk,
				VectorFromObject:             index.vectorFromObject,
				GetViewThunk:                 index.getViewThunk,
				TempVectorForIDWithViewThunk: index.tempVectorForIDWithViewThunk,
				DistanceProvider:             index.distanceProvider,
				MakeCommitLoggerThunk:        index.makeCommitLoggerThunk,
				WaitForCachePrefill:          index.hnswWaitForCachePrefill,
				AllocChecker:                 index.AllocChecker,
				MakeBucketOptions:            index.MakeBucketOptions,
				AsyncIndexingEnabled:         index.AsyncIndexingEnabled,
			},
			index.uc.HnswUC,
			index.tombstoneCallbacks,
			index.store,
		)
		if err != nil {
			return nil, err
		}
		index.index = hnsw
	} else {
		flat, err := flat.New(flatConfig, uc.FlatUC, store)
		if err != nil {
			return nil, err
		}
		index.index = flat

		if interrupted {
			if err := index.recoverInterruptedUpgrade(); err != nil {
				return nil, err
			}
		}
	}

	return index, nil
}

// recoverInterruptedUpgrade restores a consistent flat stage after an
// interrupted flat→HNSW upgrade (the in-progress-upgrade marker present on load,
// or an ordinary abort in doUpgrade). The in-progress HNSW shares the flat
// stage's on-disk compressed bucket, so a quantized upgrade that died mid-copy
// left it a mix of flat- and HNSW-format codes of different byte lengths, and
// later flat searches fail with "vector lengths don't match". The raw vectors
// bucket is untouched and complete, so every flat compressed code is re-derived
// from it, overwriting any pollution.
//
// The marker is cleared ONLY after a fully successful, durable rebuild: any
// failure returns without clearing it, so the next restart retries rather than
// leaving corruption behind a "flat, all good" state.
func (dynamic *dynamic) recoverInterruptedUpgrade() error {
	// Only a compressed flat stage reads the shared compressed bucket; an
	// uncompressed one serves from the raw bucket and ignores whatever an
	// aborted upgrade left behind.
	if dynamic.index.Compressed() {
		if err := dynamic.rebuildCompressedFromRaw(); err != nil {
			// keep the marker: a partial rebuild must be retried, not sealed
			return errors.Wrap(err, "rebuild flat compressed bucket after interrupted upgrade")
		}
		// Make the rebuilt codes durable BEFORE clearing the marker. The marker
		// lives in the shard metadata DB (fsync'd on write), while the codes go
		// to the compressed bucket's write-ahead log, which is bufio-buffered with
		// no per-Put fsync. Without this flush a crash could persist "flat, all
		// good" while losing rebuilt codes still sitting in the WAL buffer,
		// resurfacing the corruption. The rebuild is idempotent, so a crash after
		// the flush but before the marker clear just re-runs the identical repair.
		if err := dynamic.flushCompressedBucket(); err != nil {
			return errors.Wrap(err, "flush rebuilt compressed bucket after interrupted upgrade")
		}
	}

	if err := dynamic.clearUpgradingMarker(); err != nil {
		return errors.Wrap(err, "clear dynamic upgrading marker")
	}
	return nil
}

// flushCompressedBucket flushes the flat stage's shared compressed bucket to a
// durable on-disk segment. The interrupted-upgrade recovery uses it to persist
// the rebuilt codes before it clears the upgrade marker, so the ordering between
// the (fsync'd) marker and the (WAL-buffered) codes cannot invert across a crash.
func (dynamic *dynamic) flushCompressedBucket() error {
	bucket, release := dynamic.store.AcquireBucketForRead(dynamic.getCompressedBucketName())
	if bucket == nil {
		// no compressed bucket: nothing was rebuilt, nothing to flush
		return nil
	}
	defer release()
	return bucket.FlushMemtable()
}

// rebuildCompressedFromRaw re-encodes every raw vector back into the flat
// stage's compressed bucket, overwriting any foreign-format codes an aborted
// upgrade wrote there. Preload uses the flat quantizer and the raw bucket holds
// the already-normalized vectors flat.Add persisted, so the rewritten codes are
// byte-identical to the ones the flat stage produced originally.
// fallibleReindexer is the flat stage's error-returning re-encode path. Recovery
// uses it instead of Preload (which only logs) so a failed re-encode aborts the
// rebuild and keeps the upgrading marker for a retry.
type fallibleReindexer interface {
	PreloadWithErr(id uint64, vector []float32) error
}

func (dynamic *dynamic) rebuildCompressedFromRaw() error {
	reindexer, ok := dynamic.index.(fallibleReindexer)
	if !ok {
		return fmt.Errorf("expected flat index during dynamic rebuild, got %T", dynamic.index)
	}

	rawBucket, releaseRaw := dynamic.store.AcquireBucketForRead(dynamic.getBucketName())
	if rawBucket == nil {
		// no raw vectors bucket: nothing was indexed, nothing to rebuild
		return nil
	}
	defer releaseRaw()

	// Pass 1: re-encode every raw vector, overwriting any foreign-format code an
	// aborted upgrade wrote for an id that still exists.
	rawCursor := rawBucket.Cursor()
	for k, v := rawCursor.First(); k != nil; k, v = rawCursor.Next() {
		id := binary.BigEndian.Uint64(k)
		vec := make([]float32, len(v)/4)
		float32SliceFromByteSlice(v, vec)
		if err := reindexer.PreloadWithErr(id, vec); err != nil {
			rawCursor.Close()
			return errors.Wrapf(err, "re-encode vector %d", id)
		}
	}
	rawCursor.Close()

	// Pass 2: drop orphaned compressed codes — keys the aborted copy wrote for ids
	// no longer in the (delete-authoritative) raw bucket, e.g. an id deleted after
	// the copy's batch snapshot but before its AddBatch. Without this a scan of the
	// compressed bucket would resurrect the deleted id (or trip on a wrong-length
	// code). Reconcile BEFORE the durable flush and marker clear.
	compressed, releaseCompressed := dynamic.store.AcquireBucketForRead(dynamic.getCompressedBucketName())
	if compressed == nil {
		return nil
	}
	defer releaseCompressed()

	var orphans [][]byte
	compCursor := compressed.Cursor()
	for k, _ := compCursor.First(); k != nil; k, _ = compCursor.Next() {
		raw, err := rawBucket.Get(k)
		if err != nil {
			compCursor.Close()
			return errors.Wrapf(err, "read raw vector %d during reconcile", binary.BigEndian.Uint64(k))
		}
		if len(raw) == 0 {
			// copy the key: cursor keys are only valid until the next move
			orphans = append(orphans, append([]byte(nil), k...))
		}
	}
	compCursor.Close()

	for _, k := range orphans {
		if err := compressed.Delete(k); err != nil {
			return errors.Wrapf(err, "delete orphaned compressed code %d", binary.BigEndian.Uint64(k))
		}
	}
	return nil
}

func (dynamic *dynamic) Type() common.IndexType {
	return common.IndexTypeDynamic
}

func (dynamic *dynamic) dbKey() []byte {
	return dbKeyForID(dynamic.id)
}

// dbKeyForID derives the upgrade-verdict key from a physical index ID: the
// bare key for the legacy "main" index, "<key>_<suffix>" for "vectors_<suffix>".
func dbKeyForID(physicalID string) []byte {
	suffix := helpers.PhysicalIDSuffix(physicalID)
	if suffix == "" {
		return []byte(composerUpgradedKey)
	}

	key := make([]byte, 0, len(composerUpgradedKey)+len(suffix)+1)
	key = append(key, composerUpgradedKey...)
	key = append(key, '_')
	key = append(key, suffix...)
	return key
}

// RemoveStateKey deletes targetVector's flat-to-hnsw verdict and upgrading
// marker from the shard's metadata DB. No artifact list can carry them —
// index.db belongs to the shard, not to any one vector — so the sweeps that
// never load a shard clear them here. A verdict left behind is inherited by
// the next vector of the same name, which boots straight into an empty hnsw
// and never serves its flat stage.
//
// A missing or locked metadata DB is success: nothing was upgraded, or a
// loaded shard owns the key and deletes it through its own handle (both
// baked into shardmeta.DeleteOffline).
func RemoveStateKey(rootPath, targetVector string) error {
	return RemoveStateKeyForID(rootPath, helpers.VectorIndexIDForTarget(targetVector))
}

// RemoveStateKeyForID is RemoveStateKey keyed by the physical ID a record
// maps the vector to.
func RemoveStateKeyForID(rootPath, id string) error {
	if err := shardmeta.DeleteOffline(rootPath, StateNamespace, stateKeysForID(id)...); err != nil {
		return fmt.Errorf("delete dynamic state for %q: %w", id, err)
	}
	return nil
}

// stateKeysForID lists every key the dynamic index of id owns: its verdict
// and its upgrading marker. A marker left behind makes the next vector of the
// same name load as an interrupted upgrade.
func stateKeysForID(id string) [][]byte {
	return [][]byte{dbKeyForID(id), upgradingKeyForID(id)}
}

// deleteState removes every key stateKeysForID lists through state.
func deleteState(state StateOps, id string) error {
	for _, key := range stateKeysForID(id) {
		if err := state.Delete(key); err != nil {
			return err
		}
	}
	return nil
}

// upgradingKeyForID builds the in-progress-upgrade marker key in its own
// namespace ("upgrading" / "upgrading_<suffix>"), mirroring dbKeyForID's shape
// but off a distinct prefix so it can never collide with any verdict key.
func upgradingKeyForID(physicalID string) []byte {
	suffix := helpers.PhysicalIDSuffix(physicalID)
	if suffix == "" {
		return []byte(upgradingMarkerKey)
	}

	key := make([]byte, 0, len(upgradingMarkerKey)+len(suffix)+1)
	key = append(key, upgradingMarkerKey...)
	key = append(key, '_')
	key = append(key, suffix...)
	return key
}

func (dynamic *dynamic) upgradingKey() []byte {
	return upgradingKeyForID(dynamic.id)
}

// markUpgrading durably records that a flat→HNSW upgrade has started, before it
// touches any bucket. clearUpgradingMarker removes it after a committed upgrade
// or a durable repair. upgradingMarkerPresent reports whether one is recorded.
func (dynamic *dynamic) markUpgrading() error {
	return dynamic.state.Put(dynamic.upgradingKey(), []byte{1})
}

func (dynamic *dynamic) clearUpgradingMarker() error {
	return dynamic.state.Delete(dynamic.upgradingKey())
}

func (dynamic *dynamic) upgradingMarkerPresent() (bool, error) {
	v, err := dynamic.state.Get(dynamic.upgradingKey())
	return len(v) > 0, err
}

// UpgradedOnDisk reports whether the dynamic index of an unloaded shard
// already switched to hnsw, reading the same state the shard's own load
// reads: the shard metadata DB, falling back for a named vector to the hnsw
// commit log directory. An unnamed vector gets no such fallback, because its
// load reads a missing key as not upgraded and then deletes that directory.
//
// State that could not be read returns false along with the error, so a
// caller can tell that answer apart from a shard positively known to be flat.
func UpgradedOnDisk(rootPath, id string) (bool, error) {
	vals, ok, err := shardmeta.GetOfflineMulti(rootPath, StateNamespace, dbKeyForID(id), upgradingKeyForID(id))
	if err != nil {
		return false, fmt.Errorf("read dynamic state: %w", err)
	}
	if !ok {
		return upgradedFromVerdict(nil, nil, rootPath, id), nil
	}
	return upgradedFromVerdict(vals[0], vals[1], rootPath, id), nil
}

// UpgradedInState is UpgradedOnDisk through a loaded shard's own handle. It
// only reads; the index's load is what migrates a missing named key.
func UpgradedInState(state StateOps, rootPath, id string) (bool, error) {
	v, err := state.Get(dbKeyForID(id))
	if err != nil {
		return false, fmt.Errorf("read dynamic state: %w", err)
	}
	marker, err := state.Get(upgradingKeyForID(id))
	if err != nil {
		return false, fmt.Errorf("read dynamic state: %w", err)
	}
	return upgradedFromVerdict(v, marker, rootPath, id), nil
}

// RemoveStateKeyIn is RemoveStateKey through a loaded shard's own handle.
func RemoveStateKeyIn(state StateOps, id string) error {
	if err := deleteState(state, id); err != nil {
		return fmt.Errorf("delete dynamic state for %q: %w", id, err)
	}
	return nil
}

// upgradedFromVerdict decodes a verdict as the index's load does: a
// committed verdict wins over a stale upgrading marker (a crash between the
// verdict write and the marker clear); otherwise a present marker means the
// upgrade was interrupted, overriding even the named-vector dir fallback. Any
// other value wins; none falls back to the commit log directory for a named
// vector only, since an unnamed vector's load deletes that directory.
func upgradedFromVerdict(v, marker []byte, rootPath, id string) bool {
	if len(v) > 0 && v[0] == verdictUpgraded {
		return true
	}
	if len(marker) > 0 {
		return false
	}
	if len(v) > 0 {
		return v[0] == verdictUpgraded
	}
	if helpers.PhysicalIDSuffix(id) == "" {
		return false
	}
	_, err := os.Stat(hnswCommitLogDirectory(rootPath, id))
	return err == nil
}

func (dynamic *dynamic) getBucketName() string {
	return helpers.VectorsBucketNameForID(dynamic.id)
}

func (dynamic *dynamic) init(cfg *Config) (upgraded, interrupted bool, err error) {
	hnswDirExists := false
	_, statErr := os.Stat(hnswCommitLogDirectory(cfg.RootPath, cfg.ID))
	if statErr == nil {
		hnswDirExists = true
	}

	upgradingPresent, err := dynamic.upgradingMarkerPresent()
	if err != nil {
		return false, false, errors.Wrap(err, "get dynamic upgrading marker")
	}
	v, err := dynamic.state.Get(dynamic.dbKey())
	if err != nil {
		return false, false, errors.Wrap(err, "get dynamic state")
	}

	switch {
	case len(v) > 0:
		// a stored empty value reads back non-nil, so length is what says
		// whether a state was recorded
		upgraded = v[0] == verdictUpgraded
	case !upgradingPresent && helpers.PhysicalIDSuffix(cfg.ID) != "":
		// a bug in earlier versions caused named vectors to all use the same
		// key. this migrates to per-vector keys, inferring the verdict from the
		// HNSW dir. Skipped while an upgrade is in flight — its own marker, not
		// the half-written commit-log dir, is authoritative.
		verdict := []byte{verdictFlat}
		if hnswDirExists {
			verdict = []byte{verdictUpgraded}
		}
		if err := dynamic.state.Put(dynamic.dbKey(), verdict); err != nil {
			return false, false, errors.Wrap(err, "migrate dynamic state for target vector")
		}
		upgraded = hnswDirExists
	}

	if upgraded && upgradingPresent {
		// the upgrade committed (verdict wins) but crashed between writing the
		// verdict and clearing its start marker; drop the now-stale marker.
		if err := dynamic.clearUpgradingMarker(); err != nil {
			return false, false, errors.Wrap(err, "clear stale dynamic upgrading marker")
		}
	}

	// an upgrade started but never committed → roll back to the flat stage and
	// repair the shared compressed bucket the quantized upgrade may have polluted
	// (handled by New via recoverInterruptedUpgrade).
	interrupted = upgradingPresent && !upgraded

	// If not yet upgraded, remove any stale HNSW commit log left by an
	// upgrade that was aborted or crashed before completing. hnsw.New()
	// replays every file in the commit log directory, so without this
	// cleanup the next upgrade attempt inherits partial state from the
	// prior one, corrupting the rebuilt index.
	if !upgraded {
		commitLogDir := hnswCommitLogDirectory(cfg.RootPath, cfg.ID)
		if err := os.RemoveAll(commitLogDir); err != nil {
			return false, false, errors.Wrap(err, "clean up stale hnsw commit log")
		}
	}

	return upgraded, interrupted, nil
}

func (dynamic *dynamic) getCompressedBucketName() string {
	return helpers.CompressedBucketNameForID(dynamic.id)
}

func (dynamic *dynamic) Compressed() bool {
	dynamic.RLock()
	defer dynamic.RUnlock()
	return dynamic.index.Compressed()
}

func (dynamic *dynamic) Multivector() bool {
	dynamic.RLock()
	defer dynamic.RUnlock()
	return dynamic.index.Multivector()
}

func (dynamic *dynamic) AddBatch(ctx context.Context, ids []uint64, vectors [][]float32) error {
	dynamic.RLock()
	defer dynamic.RUnlock()
	return dynamic.index.AddBatch(ctx, ids, vectors)
}

func (dynamic *dynamic) Add(ctx context.Context, id uint64, vector []float32) error {
	dynamic.RLock()
	defer dynamic.RUnlock()
	return dynamic.index.Add(ctx, id, vector)
}

func (dynamic *dynamic) Delete(ids ...uint64) error {
	dynamic.RLock()
	defer dynamic.RUnlock()
	return dynamic.index.Delete(ids...)
}

func (dynamic *dynamic) SearchByVector(ctx context.Context, vector []float32, k int, allow helpers.AllowList) ([]uint64, []float32, error) {
	dynamic.RLock()
	defer dynamic.RUnlock()
	return dynamic.index.SearchByVector(ctx, vector, k, allow)
}

func (dynamic *dynamic) SearchByVectorDistance(ctx context.Context, vector []float32, targetDistance float32, maxLimit int64, allow helpers.AllowList) ([]uint64, []float32, error) {
	dynamic.RLock()
	defer dynamic.RUnlock()
	return dynamic.index.SearchByVectorDistance(ctx, vector, targetDistance, maxLimit, allow)
}

func (dynamic *dynamic) UpdateUserConfig(updated schemaconfig.VectorIndexConfig, callback func()) error {
	parsed, ok := updated.(ent.UserConfig)
	if !ok {
		callback()
		return errors.Errorf("config is not UserConfig, but %T", updated)
	}
	// doUpgrade swaps dynamic.index and flips upgraded under the exclusive lock;
	// hold it across the check and the use so an upgrade can't land in between
	// and route the wrong sub-config into the swapped index.
	dynamic.Lock()
	defer dynamic.Unlock()
	if dynamic.status.IsUpgraded() {
		return dynamic.index.UpdateUserConfig(parsed.HnswUC, callback)
	}
	dynamic.uc = parsed
	return dynamic.index.UpdateUserConfig(parsed.FlatUC, callback)
}

// Drop tears down this dynamic index. The shard-level metadata DB is NOT
// closed or removed here — it is shard-owned, shared by every dynamic vector
// on the shard and by the shard's shutdown, drop and backup paths. Closing
// it here (as this method once did) broke every sibling and made
// DebugResetVectorIndex fail on re-init, which reuses the shard's handle.
//
// With keepFiles=false this index's own state key goes, so a re-created
// vector of the same name starts from its flat stage instead of inheriting a
// stale "already upgraded" verdict. A metadata DB that is already closed is
// tolerated: that only happens when the shard was shut down first, and then
// the whole shard directory — key included — is removed right after.
func (dynamic *dynamic) Drop(ctx context.Context, keepFiles bool) error {
	if dynamic.ctx.Err() != nil {
		// already dropped
		return nil
	}

	// cancel the context before locking to stop any ongoing operations
	// and prevent new ones from starting
	dynamic.cancel()

	dynamic.Lock()
	defer dynamic.Unlock()

	if !keepFiles {
		if err := deleteState(dynamic.state, dynamic.id); err != nil && !shardmeta.IsClosed(err) {
			return fmt.Errorf("delete dynamic state for %q: %w", dynamic.id, err)
		}
	}

	return dynamic.index.Drop(ctx, keepFiles)
}

// DropTargetVector removes ONE named vector's dynamic index, leaving the shard
// intact. Drop is unusable here: the state DB is shared by every dynamic vector
// on the shard (one handle, one key per vector), so its Close()+Remove would
// take every sibling's state with it. This deletes only this vector's key,
// which is also what stops a re-created vector of the same name inheriting a
// stale "already upgraded" verdict.
//
// Loaded-shard route only: the files-only sweeps never open the state DB, so a
// drop on a cold shard leaves the key behind.
func (dynamic *dynamic) DropTargetVector(ctx context.Context) error {
	if dynamic.ctx.Err() != nil {
		// already dropped
		return nil
	}

	dynamic.cancel()

	dynamic.Lock()
	defer dynamic.Unlock()

	if err := deleteState(dynamic.state, dynamic.id); err != nil {
		return fmt.Errorf("delete dynamic state for %q: %w", dynamic.id, err)
	}

	// keepFiles=false: the underlying index's own files go, but the SHARED
	// state DB above is untouched by this path.
	return dynamic.index.Drop(ctx, false)
}

func (dynamic *dynamic) Flush() error {
	dynamic.RLock()
	defer dynamic.RUnlock()
	return dynamic.index.Flush()
}

func (dynamic *dynamic) Shutdown(ctx context.Context) error {
	if dynamic.ctx.Err() != nil {
		// already closed
		return nil
	}

	// cancel the context before locking to stop any ongoing operations
	// and prevent new ones from starting
	dynamic.cancel()

	dynamic.Lock()
	defer dynamic.Unlock()

	return dynamic.index.Shutdown(ctx)
}

func (dynamic *dynamic) PrepareForBackup(ctx context.Context) error {
	dynamic.RLock()
	defer dynamic.RUnlock()
	return dynamic.index.PrepareForBackup(ctx)
}

func (dynamic *dynamic) ResumeAfterBackup(ctx context.Context) error {
	dynamic.RLock()
	defer dynamic.RUnlock()
	return dynamic.index.ResumeAfterBackup(ctx)
}

func (dynamic *dynamic) ListFiles(ctx context.Context, basePath string) ([]string, error) {
	dynamic.RLock()
	defer dynamic.RUnlock()
	return dynamic.index.ListFiles(ctx, basePath)
}

// SnapshotMutableFiles delegates to the underlying index. The shard-level
// metadata DB (index.db) is NOT snapshotted here — the shard snapshots it
// once per shard itself (Shard.CreateBackupSnapshot via shardmeta.DB.Snapshot)
// rather than through this per-index method, which the shard's
// ForEachVectorIndex would otherwise invoke once per named vector and thus
// duplicate the copy and its sd.Files entry.
func (dynamic *dynamic) SnapshotMutableFiles(ctx context.Context, basePath, stagingDir string) ([]string, error) {
	dynamic.RLock()
	defer dynamic.RUnlock()
	return dynamic.index.SnapshotMutableFiles(ctx, basePath, stagingDir)
}

func (dynamic *dynamic) ValidateBeforeInsert(vector []float32) error {
	dynamic.RLock()
	defer dynamic.RUnlock()
	return dynamic.index.ValidateBeforeInsert(vector)
}

func (dynamic *dynamic) PostStartup(ctx context.Context) {
	dynamic.Lock()
	defer dynamic.Unlock()
	dynamic.index.PostStartup(ctx)
}

func (dynamic *dynamic) ContainsDoc(docID uint64) bool {
	dynamic.RLock()
	defer dynamic.RUnlock()
	return dynamic.index.ContainsDoc(docID)
}

func (dynamic *dynamic) Preload(id uint64, vector []float32) {
	dynamic.RLock()
	defer dynamic.RUnlock()
	dynamic.index.Preload(id, vector)
}

func (dynamic *dynamic) AlreadyIndexed() uint64 {
	dynamic.RLock()
	defer dynamic.RUnlock()
	return dynamic.index.(upgradableIndexer).AlreadyIndexed()
}

func (dynamic *dynamic) QueryVectorDistancer(queryVector []float32) common.QueryVectorDistancer {
	dynamic.RLock()
	defer dynamic.RUnlock()
	return dynamic.index.QueryVectorDistancer(queryVector)
}

func (dynamic *dynamic) ShouldUpgrade() (bool, int) {
	if !dynamic.status.IsUpgraded() {
		return true, int(dynamic.threshold)
	}
	dynamic.RLock()
	defer dynamic.RUnlock()
	return dynamic.index.(upgradableIndexer).ShouldUpgrade()
}

func (dynamic *dynamic) Upgraded() bool {
	dynamic.RLock()
	defer dynamic.RUnlock()
	return dynamic.status.IsUpgraded() && dynamic.index.(upgradableIndexer).Upgraded()
}

// UpgradeInProgress reports a flat→HNSW restructure in flight, or (post-upgrade)
// an in-flight compression on the inner HNSW.
func (dynamic *dynamic) UpgradeInProgress() bool {
	if dynamic.status.IsUpgrading() {
		return true
	}
	if !dynamic.status.IsUpgraded() {
		return false
	}
	dynamic.RLock()
	defer dynamic.RUnlock()
	if u, ok := dynamic.index.(upgradableIndexer); ok {
		return u.UpgradeInProgress()
	}
	return false
}

func float32SliceFromByteSlice(vector []byte, slice []float32) []float32 {
	byteops.CopyBytesToSlice(slice, vector[:len(slice)*4])
	return slice
}

func (dynamic *dynamic) Upgrade(callback func()) error {
	if dynamic.ctx.Err() != nil {
		// no goroutine will run to fire the callback, so the pause/resume
		// contract must be resolved synchronously here.
		callback()
		return dynamic.ctx.Err()
	}

	if dynamic.status.IsUpgraded() {
		return dynamic.index.(upgradableIndexer).Upgrade(callback)
	}

	if !dynamic.status.TryUpgrading() {
		// an attempt is already in flight (or just finished) and owns its own
		// callback; resolve this caller's separately instead of blocking on it.
		callback()
		return nil
	}

	enterrors.GoWrapper(func() {
		defer callback()
		// re-arm on error AND panic (GoWrapper recovers): a later Upgrade call
		// must be able to retry. doUpgrade flips status only on success.
		defer func() {
			if !dynamic.status.IsUpgraded() {
				dynamic.status.Reset()
			}
		}()
		dynamic.logger.Debugf("upgrade to HNSW started")

		err := dynamic.upgradeFn()
		if err != nil {
			dynamic.logger.Errorf("failed to upgrade index: %v", err)
			return
		}
		dynamic.logger.Debugf("upgrade to HNSW completed")
	}, dynamic.logger)

	return nil
}

// doUpgrade is the production upgrade entry (wired into upgradeFn by New). Tests
// override upgradeFn to call upgradeUsing with a wrapper that interrupts the copy.
func (dynamic *dynamic) doUpgrade() error {
	return dynamic.upgradeUsing(nil)
}

// upgradeUsing performs the flat→HNSW upgrade. wrapCopyTarget, when non-nil,
// wraps the freshly built HNSW before the copy so a test can intercept AddBatch
// (panic, error, or a concurrent delete); production passes nil. The wrapper is
// only the copy destination — the real HNSW is what gets started and swapped in.
func (dynamic *dynamic) upgradeUsing(wrapCopyTarget func(VectorIndex) VectorIndex) error {
	// Persist the in-progress-upgrade marker BEFORE building the HNSW or touching
	// any bucket. From here on the new HNSW writes its (possibly different-length)
	// quantized codes into the compressed bucket the flat stage shares, so an
	// interruption before the commit below must be recoverable: the marker's
	// presence on the next load (or on an ordinary abort here) drives the flat
	// stage to rebuild that bucket from the raw vectors.
	// See https://github.com/weaviate/0-weaviate-issues/issues/451.
	if err := dynamic.markUpgrading(); err != nil {
		return errors.Wrap(err, "mark dynamic upgrade in progress")
	}

	// The read lock keeps the current index alive (not dropped/closed) while
	// the new one is built; searches continue throughout. Closure so the
	// unlock is deferred and panic-safe.
	index, err := func() (*hnsw.HNSW, error) {
		dynamic.RLock()
		defer dynamic.RUnlock()

		index, err := hnsw.New(
			hnsw.Config{
				Logger:                       dynamic.logger,
				RootPath:                     dynamic.rootPath,
				ID:                           dynamic.id,
				ShardName:                    dynamic.shardName,
				ClassName:                    dynamic.className,
				PrometheusMetrics:            dynamic.prometheusMetrics,
				VectorForIDThunk:             dynamic.vectorForIDThunk,
				VectorFromObject:             dynamic.vectorFromObject,
				GetViewThunk:                 dynamic.getViewThunk,
				TempVectorForIDWithViewThunk: dynamic.tempVectorForIDWithViewThunk,
				DistanceProvider:             dynamic.distanceProvider,
				MakeCommitLoggerThunk:        dynamic.makeCommitLoggerThunk,
				WaitForCachePrefill:          dynamic.hnswWaitForCachePrefill,
				AllocChecker:                 dynamic.AllocChecker,
				MakeBucketOptions:            dynamic.MakeBucketOptions,
				AsyncIndexingEnabled:         dynamic.AsyncIndexingEnabled,
			},
			dynamic.uc.HnswUC,
			dynamic.tombstoneCallbacks,
			dynamic.store,
		)
		if err != nil {
			return nil, err
		}

		// The copy destination may be wrapped (tests only) to interrupt AddBatch;
		// the real index is still what PostStartup + the swap below use.
		var copyTarget VectorIndex = index
		if wrapCopyTarget != nil {
			copyTarget = wrapCopyTarget(index)
		}
		if err := dynamic.copyToVectorIndex(copyTarget); err != nil {
			dynamic.cleanupAbortedUpgrade(index)
			return nil, err
		}

		// Start commit-log maintenance on the new HNSW index. The cache prefill
		// is a no-op because cachePrefilled was already set during init (fresh
		// index with no commit-log state) and the cache is populated by AddBatch.
		index.PostStartup(dynamic.ctx)
		return index, nil
	}()
	if err != nil {
		// Ordinary (non-crash) upgrade failure: hnsw.New or the copy errored and
		// the flat stage stays live. Any copied batch may have written
		// HNSW-format codes into the shared compressed bucket, so recover it in
		// place — rebuild from raw, durable flush, clear the marker — before the
		// flat index resumes serving. Skip while shutting down (ctx cancelled):
		// the store is tearing down and the next start recovers from the marker.
		// Best-effort: on failure the marker survives, so a restart retries.
		//
		// Under the EXCLUSIVE lock: the rebuild mutates the shared compressed
		// bucket, so concurrent Add/Delete/Search (which hold RLock) must be
		// serialized against it — otherwise a search could read a half-rewritten
		// bucket. Safe against the upgrade path's own locking: the build closure
		// above already released its RLock, and the commit below is not reached on
		// this error path, so no dynamic lock is held here.
		if dynamic.ctx.Err() == nil {
			dynamic.Lock()
			rerr := dynamic.recoverInterruptedUpgrade()
			dynamic.Unlock()
			if rerr != nil {
				dynamic.logger.WithField("action", "dynamic_upgrade_abort").
					Errorf("recover flat stage after aborted upgrade: %v", rerr)
			}
		}
		return err
	}

	// Lock the index for writing but check if it was already
	// closed in the meantime
	dynamic.Lock()
	defer dynamic.Unlock()

	if err := dynamic.ctx.Err(); err != nil {
		// already closed
		dynamic.cleanupAbortedUpgrade(index)
		return errors.Wrap(err, "index was closed while upgrading")
	}

	// commit the verdict; the in-progress marker is cleared right after. Order
	// matters: verdict first, so a crash between the two leaves verdict {1} + a
	// stale marker, which init reads as a committed upgrade (marker dropped as
	// stale) rather than an interrupted one.
	err = dynamic.state.Put(dynamic.dbKey(), []byte{verdictUpgraded})
	if err != nil {
		// the new index is never installed, so tear it down like any other
		// aborted upgrade. The in-progress marker stays put, so a restart rolls
		// back to a consistent flat stage.
		dynamic.cleanupAbortedUpgrade(index)
		return errors.Wrap(err, "update dynamic")
	}
	if err := dynamic.clearUpgradingMarker(); err != nil {
		// non-fatal: verdict {1} already committed the upgrade; a lingering
		// marker is dropped as stale on the next load.
		dynamic.logger.WithField("action", "dynamic_upgrade_commit").
			Errorf("clear dynamic upgrading marker after commit: %v", err)
	}

	dynamic.index.Drop(dynamic.ctx, false)
	dynamic.index = index
	dynamic.status.Upgraded()

	var errs []error
	if bDir, err := dynamic.bucketDir(dynamic.getBucketName()); err != nil {
		errs = append(errs, err)
	} else {
		if err := dynamic.store.ShutdownBucket(dynamic.ctx, dynamic.getBucketName()); err != nil {
			errs = append(errs, err)
		}
		if err := os.RemoveAll(bDir); err != nil {
			errs = append(errs, err)
		}
	}
	// Due to the potential for a different quantizer using a different endianness
	// we remove the bucket here if needed
	removeCompressedBucket := false
	if dynamic.uc.FlatUC.BQ.Enabled || dynamic.uc.FlatUC.RQ.Enabled {
		if !dynamic.uc.HnswUC.BQ.Enabled && !dynamic.uc.HnswUC.RQ.Enabled {
			removeCompressedBucket = true
		}
	}

	if removeCompressedBucket {
		if bDir, err := dynamic.bucketDir(dynamic.getCompressedBucketName()); err != nil {
			errs = append(errs, err)
		} else {
			if err := dynamic.store.ShutdownBucket(dynamic.ctx, dynamic.getCompressedBucketName()); err != nil {
				errs = append(errs, err)
			}
			if err := os.RemoveAll(bDir); err != nil {
				errs = append(errs, err)
			}
		}
	}
	if len(errs) > 0 {
		dynamic.logger.Warn(simpleErrors.Join(errs...))
	}

	return nil
}

// cleanupAbortedUpgrade tears down a partially-built HNSW index after an
// aborted flat->HNSW upgrade. The commit log written so far must not stay on
// disk: the next hnsw.New() (upgrade retry or shard restart) replays every
// file in the commit log directory, so leftover partial state would corrupt
// the rebuilt index. Uses a fresh context because the abort is typically
// caused by dynamic.ctx being canceled.
func (dynamic *dynamic) cleanupAbortedUpgrade(index VectorIndex) {
	if err := index.Drop(context.Background(), false); err != nil {
		dynamic.logger.WithField("action", "dynamic_upgrade_abort").
			Errorf("drop partially-built hnsw index: %v", err)
	}
	// Drop removes the commit log directory, but remove it explicitly in case
	// Drop failed partway through.
	if err := os.RemoveAll(hnswCommitLogDirectory(dynamic.rootPath, dynamic.id)); err != nil {
		dynamic.logger.WithField("action", "dynamic_upgrade_abort").
			Errorf("remove partial hnsw commit log: %v", err)
	}
}

// bucketDir resolves a bucket's on-disk directory. The upgrade's cleanup runs
// on a background goroutine, so a shard teardown can deregister the bucket
// first — in which case there is nothing left to shut down or delete, and the
// lookup must report that instead of dereferencing nil. The pin is dropped
// before returning: the caller's next step is a Shutdown of this same bucket,
// which drains the very pin we would otherwise still hold.
func (dynamic *dynamic) bucketDir(name string) (string, error) {
	bucket, release := dynamic.store.AcquireBucketForRead(name)
	if bucket == nil {
		return "", fmt.Errorf("dynamic index: bucket %q: %w", name, lsmkv.ErrBucketNotFound)
	}
	defer release()

	return bucket.GetDir(), nil
}

// Loop over the store and add each vector to the HNSW.
// This can take a while, so we use short-lived cursors to not block
// other operations on the KV store (e.g. flush)
func (dynamic *dynamic) copyToVectorIndex(index VectorIndex) error {
	bucketName := dynamic.getBucketName()

	var k, v []byte

	var ids []uint64
	var vectors [][]float32

	for {
		ids = ids[:0]
		vectors = vectors[:0]

		// re-acquired per batch, released with the cursor: a pin spanning the
		// whole copy would hold off a teardown for as long as the upgrade
		// runs, which is exactly what the short-lived cursors avoid
		bucket, release := dynamic.store.AcquireBucketForRead(bucketName)
		if bucket == nil {
			return fmt.Errorf("copy vectors to hnsw: bucket %q: %w", bucketName, lsmkv.ErrBucketNotFound)
		}

		cursor := bucket.Cursor()

		if len(k) == 0 {
			k, v = cursor.First()
		} else {
			k, v = cursor.Seek(k)
		}

		var i int
		for k != nil && i < batchSize {
			if err := dynamic.ctx.Err(); err != nil {
				cursor.Close()
				release()
				// context was cancelled, stop processing
				return err
			}

			id := binary.BigEndian.Uint64(k)
			vc := make([]float32, len(v)/4)
			float32SliceFromByteSlice(v, vc)

			ids = append(ids, id)
			vectors = append(vectors, vc)

			k, v = cursor.Next()
			i++
		}

		cursor.Close()
		release()

		if err := index.AddBatch(dynamic.ctx, ids, vectors); err != nil {
			return errors.Wrap(err, "add vectors to upgraded index")
		}

		if k == nil {
			break
		}
	}

	return nil
}

func (dynamic *dynamic) Iterate(fn func(id uint64) bool) {
	dynamic.RLock()
	defer dynamic.RUnlock()
	dynamic.index.Iterate(fn)
}

type hnswStats interface {
	Stats() (*hnsw.HnswStats, error)
}

func (dynamic *dynamic) Stats() (*hnsw.HnswStats, error) {
	dynamic.RLock()
	defer dynamic.RUnlock()

	h, ok := dynamic.index.(hnswStats)
	if !ok {
		return nil, errors.New("index is not hnsw")
	}
	return h.Stats()
}

type compressionStatsReporter interface {
	CompressionStats() compressionhelpers.CompressionStats
}

func (dynamic *dynamic) CompressionStats() compressionhelpers.CompressionStats {
	dynamic.RLock()
	defer dynamic.RUnlock()

	// the stats belong to whichever index is active, flat or hnsw
	if index, ok := dynamic.index.(compressionStatsReporter); ok {
		return index.CompressionStats()
	}

	return compressionhelpers.UncompressedStats{}
}

// UnderlyingIndex returns the underlying index type (flat or hnsw)
// for dynamic indexes.
func (dynamic *dynamic) UnderlyingIndex() common.IndexType {
	dynamic.RLock()
	defer dynamic.RUnlock()
	return dynamic.index.Type()
}

func (dynamic *dynamic) IsUpgraded() bool {
	dynamic.RLock()
	defer dynamic.RUnlock()
	return dynamic.status.IsUpgraded()
}

type DynamicStats struct{}

func (s *DynamicStats) IndexType() common.IndexType {
	return common.IndexTypeDynamic
}

func hnswCommitLogDirectory(rootPath, name string) string {
	return fmt.Sprintf("%s/%s.hnsw.commitlog.d", rootPath, name)
}

// to make sure the dynamic index satisfies the Index interface
var _ = Index(&dynamic{})
