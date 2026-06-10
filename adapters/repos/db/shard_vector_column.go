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
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/columnar"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/storobj"
	hnswent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// Vector-column storage for rescoring (hnsw columnarRescore).
//
// When the hnsw config enables columnarRescore for a target vector, the
// shard maintains a dedicated columnar LSM bucket per target vector that
// stores the uncompressed float32 vectors keyed by docID. Compressed-index
// rescoring (and MUVERA exact reranking) then reads candidate vectors from
// this column instead of unmarshalling full object blobs from the objects
// bucket via GetBySecondary.
//
// Lifecycle:
//   - Writes feed the column synchronously inside putObjectLSM /
//     mergeObjectInStorage (NOT via the async vector queue — rescore would
//     otherwise race ahead of the column).
//   - Reads only serve from the column once it is "ready": a sentinel file
//     "<bucket>.backfilled" exists next to the bucket dir. The sentinel is
//     written exclusively by the backfill task, which iterates the objects
//     bucket and (idempotently) re-puts every vector. A fresh, empty shard
//     trivially completes the backfill with zero objects.
//   - Every read has a per-docID fallback to the objects bucket, so a
//     column miss (docID written before the flag was enabled and never
//     backfilled, or mid-disable gap) is never a correctness problem —
//     vectors are immutable per docID (a vector change allocates a new
//     docID, see determineInsertStatus).
//
// Consistency notes:
//   - Because vectors are immutable per docID, concurrent backfill and live
//     writes can only ever write identical payloads for the same docID.
//   - A delete racing the backfill can leave a stale column entry for a
//     docID whose object is gone (backfill reads the object from its
//     cursor, the object is deleted, then the backfill put lands after the
//     ColumnarDelete). This is benign: the vector index tombstones the
//     docID, so no search path asks for it; the entry is dead weight until
//     compaction of a future delete.
//   - Disabling the flag stops writes and reads immediately (reads via the
//     per-call enabled check in the injected thunks); the column simply
//     goes stale for docIDs created while disabled, which the per-docID
//     fallback covers on re-enable. Enabling the flag on a running shard
//     starts the backfill immediately, but the column-backed read thunks
//     are only wired at shard (re)start — reads keep using the objects
//     bucket until the next restart if the flag was off at init.
type vectorColumnState struct {
	// enabled mirrors the hnsw config's columnarRescore flag for this
	// target. Gates both writes (feed the column) and reads (serve from
	// the column).
	enabled atomic.Bool
	// ready is true once the sentinel file exists, i.e. the column holds
	// every vector that existed when the backfill scanned the objects
	// bucket (live writes cover everything newer).
	ready atomic.Bool
	// created is true once the bucket is registered in the shard's store.
	// Only ever transitions false→true, enabling a lock-free fast path.
	created atomic.Bool
	// backfillRunning dedupes concurrent backfill launches (shard init +
	// config update).
	backfillRunning atomic.Bool
	// createLock serializes bucket creation.
	createLock sync.Mutex
}

// vectorColumnSentinel is the JSON payload of the "<bucket>.backfilled"
// sentinel file. Dims/Multi let a restarting shard reload the bucket
// (columnar buckets need their schema at load time) without rescanning the
// objects bucket. Dims == 0 means the backfill saw no vectors and no bucket
// was created.
type vectorColumnSentinel struct {
	Dims  int  `json:"dims"`
	Multi bool `json:"multi"`
}

// hnswColumnarRescoreConfig extracts (enabled, multiVector) from a vector
// index config. Only hnsw supports columnarRescore: the flat index already
// serves uncompressed vectors from its own bucket, and the dynamic index
// delegates to flat/hnsw underneath (no shard-level wiring of its own).
func hnswColumnarRescoreConfig(cfg schemaConfig.VectorIndexConfig) (enabled, multi bool) {
	hnswCfg, ok := cfg.(hnswent.UserConfig)
	if !ok {
		return false, false
	}
	return hnswCfg.ColumnarRescore && !hnswCfg.Skip, hnswCfg.IsMultiVector()
}

// getVectorColumnState returns the per-target state, creating it if needed.
func (s *Shard) getVectorColumnState(targetVector string) *vectorColumnState {
	s.vectorColumnsMu.Lock()
	defer s.vectorColumnsMu.Unlock()
	if s.vectorColumns == nil {
		s.vectorColumns = map[string]*vectorColumnState{}
	}
	st, ok := s.vectorColumns[targetVector]
	if !ok {
		st = &vectorColumnState{}
		s.vectorColumns[targetVector] = st
	}
	return st
}

// peekVectorColumnState returns the per-target state without creating it.
func (s *Shard) peekVectorColumnState(targetVector string) *vectorColumnState {
	s.vectorColumnsMu.RLock()
	defer s.vectorColumnsMu.RUnlock()
	return s.vectorColumns[targetVector]
}

func (s *Shard) vectorColumnSentinelPath(targetVector string) string {
	return filepath.Join(s.pathLSM(),
		helpers.GetVectorColumnBucketName(targetVector)+".backfilled")
}

// initVectorColumns enables the vector column (and kicks off the idempotent
// backfill) for every target vector whose hnsw config has columnarRescore
// set. Called at the end of initShardVectors; must run after initNonVector
// so the objects bucket exists.
func (s *Shard) initVectorColumns() {
	if s.index.vectorIndexUserConfig != nil {
		if enabled, multi := hnswColumnarRescoreConfig(s.index.vectorIndexUserConfig); enabled {
			s.enableVectorColumn("", multi)
		}
	}
	for targetVector, cfg := range s.index.vectorIndexUserConfigs {
		if enabled, multi := hnswColumnarRescoreConfig(cfg); enabled {
			s.enableVectorColumn(targetVector, multi)
		}
	}
}

// enableVectorColumn marks the target's column as write-enabled and starts
// the backfill task unless one is already running. Safe to call repeatedly
// (shard init, config updates).
func (s *Shard) enableVectorColumn(targetVector string, multi bool) {
	st := s.getVectorColumnState(targetVector)
	st.enabled.Store(true)
	if !st.backfillRunning.CompareAndSwap(false, true) {
		return
	}
	enterrors.GoWrapper(func() {
		defer st.backfillRunning.Store(false)
		if err := s.backfillVectorColumn(s.shutCtx, targetVector, multi); err != nil {
			s.index.logger.WithFields(logrus.Fields{
				"action":        "vector_column_backfill",
				"shard":         s.ID(),
				"target_vector": targetVector,
			}).Error(fmt.Errorf("vector column backfill: %w", err))
		}
	}, s.index.logger)
}

// applyVectorColumnConfig reacts to a runtime vector index config update.
// Enabling starts the (idempotent) backfill so the column is complete and
// ready; note that the column-backed read thunks are wired at shard init,
// so reads start serving from the column after the next shard restart.
// Disabling stops writes and reads immediately via the per-call enabled
// check.
func (s *Shard) applyVectorColumnConfig(targetVector string, cfg schemaConfig.VectorIndexConfig) {
	enabled, multi := hnswColumnarRescoreConfig(cfg)
	if enabled {
		s.enableVectorColumn(targetVector, multi)
		return
	}
	if st := s.peekVectorColumnState(targetVector); st != nil {
		st.enabled.Store(false)
	}
}

// ensureVectorColumnBucket creates (or loads from disk) the columnar bucket
// for the target vector. Idempotent; cheap once created (single atomic
// load). dims/multi define the column schema on first creation — both the
// live write path (dims from the incoming vector) and the backfill (dims
// from the first scanned object) call this lazily because the
// dimensionality is unknown until the first vector is seen.
func (s *Shard) ensureVectorColumnBucket(ctx context.Context, targetVector string,
	dims int, multi bool,
) (*lsmkv.Bucket, error) {
	bucketName := helpers.GetVectorColumnBucketName(targetVector)
	st := s.getVectorColumnState(targetVector)
	if st.created.Load() {
		return s.store.Bucket(bucketName), nil
	}

	st.createLock.Lock()
	defer st.createLock.Unlock()
	if st.created.Load() {
		return s.store.Bucket(bucketName), nil
	}
	if dims <= 0 {
		return nil, fmt.Errorf("vector column bucket %q: invalid dims %d", bucketName, dims)
	}

	colName := targetVector
	if colName == "" {
		colName = "vector"
	}
	col := columnar.Column{
		Name:     colName,
		Type:     columnar.ColumnTypeVector,
		Encoding: columnar.EncodingRawFixedWidth,
		Dims:     uint32(dims),
	}
	if multi {
		col.Type = columnar.ColumnTypeMultiVector
		col.Encoding = columnar.EncodingOffsetValues
	}
	schema := &columnar.Schema{Columns: []columnar.Column{col}}

	err := s.store.CreateOrLoadBucket(ctx, bucketName,
		s.makeDefaultBucketOptions(lsmkv.StrategyColumnar,
			lsmkv.WithColumnarSchema(schema))...)
	if err != nil {
		return nil, fmt.Errorf("create vector column bucket %q: %w", bucketName, err)
	}
	st.created.Store(true)

	// If the backfill already completed on an empty shard, its sentinel
	// recorded dims=0 (no bucket existed yet). Refresh it so a restart can
	// reload this bucket (columnar buckets need their schema at load time)
	// without waiting for the first post-restart write.
	if st.ready.Load() {
		if err := writeVectorColumnSentinel(s.vectorColumnSentinelPath(targetVector),
			vectorColumnSentinel{Dims: dims, Multi: multi}); err != nil {
			s.index.logger.WithFields(logrus.Fields{
				"action":        "vector_column_sentinel",
				"shard":         s.ID(),
				"target_vector": targetVector,
			}).Error(fmt.Errorf("refresh vector column sentinel: %w", err))
		}
	}

	return s.store.Bucket(bucketName), nil
}

// backfillVectorColumn populates the column from the objects bucket and
// writes the sentinel. Idempotent: re-puts are same-value rewrites (vectors
// are immutable per docID), and a crash before the sentinel write simply
// re-runs the scan on the next start. If a valid sentinel already exists,
// it only reloads the bucket (schema from the sentinel) and flips ready.
func (s *Shard) backfillVectorColumn(ctx context.Context, targetVector string, multi bool) error {
	st := s.getVectorColumnState(targetVector)
	if st.ready.Load() {
		return nil
	}

	sentinelPath := s.vectorColumnSentinelPath(targetVector)
	if data, err := os.ReadFile(sentinelPath); err == nil {
		var sentinel vectorColumnSentinel
		if err := json.Unmarshal(data, &sentinel); err == nil {
			if sentinel.Dims > 0 {
				if _, err := s.ensureVectorColumnBucket(ctx, targetVector,
					sentinel.Dims, sentinel.Multi); err != nil {
					return err
				}
			}
			st.ready.Store(true)
			return nil
		}
		// corrupt sentinel (e.g. torn write): fall through to a full
		// backfill, which rewrites it
	}

	objBucket := s.store.Bucket(helpers.ObjectsBucketLSM)
	if objBucket == nil {
		return fmt.Errorf("objects bucket not found")
	}

	dims := 0
	var colBucket *lsmkv.Bucket
	cursor := objBucket.Cursor()
	defer cursor.Close()

	i := 0
	for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
		if i%256 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		i++

		docID, err := storobj.DocIDFromBinary(v)
		if err != nil {
			// corrupt blob: skip — the object read path will surface this
			continue
		}

		if multi {
			vecs, err := storobj.MultiVectorFromBinary(v, nil, targetVector)
			if err != nil || len(vecs) == 0 || len(vecs[0]) == 0 {
				continue // object has no vector for this target
			}
			if colBucket == nil {
				dims = len(vecs[0])
				if colBucket, err = s.ensureVectorColumnBucket(ctx, targetVector, dims, true); err != nil {
					return err
				}
			}
			if err := colBucket.ColumnarPutMultiVector(docID, vecs); err != nil {
				return fmt.Errorf("backfill multi vector for doc %d: %w", docID, err)
			}
		} else {
			vec, err := storobj.VectorFromBinary(v, nil, targetVector)
			if err != nil || len(vec) == 0 {
				continue // object has no vector for this target
			}
			if colBucket == nil {
				dims = len(vec)
				if colBucket, err = s.ensureVectorColumnBucket(ctx, targetVector, dims, false); err != nil {
					return err
				}
			}
			if err := colBucket.ColumnarPutVector(docID, vec); err != nil {
				return fmt.Errorf("backfill vector for doc %d: %w", docID, err)
			}
		}
	}

	// If the scan saw no vectors but a concurrent live write already
	// created the bucket, record that bucket's dims instead of 0 so a
	// restart can reload the bucket from the sentinel alone.
	if dims == 0 && st.created.Load() {
		if b := s.store.Bucket(helpers.GetVectorColumnBucketName(targetVector)); b != nil {
			if schema := b.ColumnarSchema(); schema != nil && schema.IsVector() {
				dims = int(schema.Columns[0].Dims)
				multi = schema.Columns[0].Type == columnar.ColumnTypeMultiVector
			}
		}
	}

	if err := writeVectorColumnSentinel(sentinelPath, vectorColumnSentinel{
		Dims:  dims,
		Multi: multi,
	}); err != nil {
		return err
	}
	st.ready.Store(true)

	s.index.logger.WithFields(logrus.Fields{
		"action":        "vector_column_backfill",
		"shard":         s.ID(),
		"target_vector": targetVector,
		"objects":       i,
	}).Debug("vector column backfill complete")
	return nil
}

// writeVectorColumnSentinel writes the sentinel atomically (tmp + rename)
// so a torn write can never be mistaken for a completed backfill.
func writeVectorColumnSentinel(path string, sentinel vectorColumnSentinel) error {
	data, err := json.Marshal(sentinel)
	if err != nil {
		return fmt.Errorf("marshal vector column sentinel: %w", err)
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("write vector column sentinel: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("rename vector column sentinel: %w", err)
	}
	return nil
}

// putVectorColumnsLSM is the synchronous write hook called from
// putObjectLSM / mergeObjectInStorage (under the per-UUID docID lock, after
// the objects-bucket upsert). It mirrors the objects bucket's docID
// semantics: payloads are written under the NEW docID; if the docID
// changed, the old docID's column entries are tombstoned.
func (s *Shard) putVectorColumnsLSM(ctx context.Context, obj *storobj.Object,
	status objectInsertStatus,
) error {
	s.vectorColumnsMu.RLock()
	noColumns := len(s.vectorColumns) == 0
	s.vectorColumnsMu.RUnlock()
	if noColumns {
		return nil
	}

	if status.docIDChanged {
		if err := s.deleteVectorColumnsLSM(status.oldDocID); err != nil {
			return err
		}
	}

	if len(obj.Vector) > 0 {
		if err := s.putVectorColumnVec(ctx, "", status.docID, obj.Vector); err != nil {
			return err
		}
	}
	for targetVector, vec := range obj.Vectors {
		if len(vec) == 0 {
			continue
		}
		if err := s.putVectorColumnVec(ctx, targetVector, status.docID, vec); err != nil {
			return err
		}
	}
	for targetVector, vecs := range obj.MultiVectors {
		if len(vecs) == 0 || len(vecs[0]) == 0 {
			continue
		}
		if err := s.putVectorColumnMultiVec(ctx, targetVector, status.docID, vecs); err != nil {
			return err
		}
	}
	return nil
}

func (s *Shard) putVectorColumnVec(ctx context.Context, targetVector string,
	docID uint64, vec []float32,
) error {
	st := s.peekVectorColumnState(targetVector)
	if st == nil || !st.enabled.Load() {
		return nil
	}
	bucket, err := s.ensureVectorColumnBucket(ctx, targetVector, len(vec), false)
	if err != nil {
		return err
	}
	if err := bucket.ColumnarPutVector(docID, vec); err != nil {
		return fmt.Errorf("vector column put for target %q: %w", targetVector, err)
	}
	return nil
}

func (s *Shard) putVectorColumnMultiVec(ctx context.Context, targetVector string,
	docID uint64, vecs [][]float32,
) error {
	st := s.peekVectorColumnState(targetVector)
	if st == nil || !st.enabled.Load() {
		return nil
	}
	bucket, err := s.ensureVectorColumnBucket(ctx, targetVector, len(vecs[0]), true)
	if err != nil {
		return err
	}
	if err := bucket.ColumnarPutMultiVector(docID, vecs); err != nil {
		return fmt.Errorf("vector column put for target %q: %w", targetVector, err)
	}
	return nil
}

// deleteVectorColumnsLSM tombstones docID in every loaded vector-column
// bucket of the shard. Called on object delete and on docID-changing
// updates (old docID). Intentionally NOT gated on the enabled flag: while
// the flag is disabled, propagating deletes keeps the column from
// resurrecting stale entries on re-enable.
func (s *Shard) deleteVectorColumnsLSM(docID uint64) error {
	s.vectorColumnsMu.RLock()
	if len(s.vectorColumns) == 0 {
		s.vectorColumnsMu.RUnlock()
		return nil
	}
	targets := make([]string, 0, len(s.vectorColumns))
	for targetVector, st := range s.vectorColumns {
		if st.created.Load() {
			targets = append(targets, targetVector)
		}
	}
	s.vectorColumnsMu.RUnlock()

	for _, targetVector := range targets {
		bucket := s.store.Bucket(helpers.GetVectorColumnBucketName(targetVector))
		if bucket == nil {
			continue
		}
		if err := bucket.ColumnarDelete(docID); err != nil {
			return fmt.Errorf("vector column delete for target %q: %w", targetVector, err)
		}
	}
	return nil
}

// servableVectorColumnBucket returns the column bucket iff reads may serve
// from it: flag enabled, backfill complete, bucket loaded. Nil means "fall
// back to the objects bucket".
func (s *Shard) servableVectorColumnBucket(targetVector string) *lsmkv.Bucket {
	st := s.peekVectorColumnState(targetVector)
	if st == nil || !st.enabled.Load() || !st.ready.Load() || !st.created.Load() {
		return nil
	}
	return s.store.Bucket(helpers.GetVectorColumnBucketName(targetVector))
}

// readVectorColumnIntoSliceWithView is the column-backed twin of
// readVectorByIndexIDIntoSliceWithView, with a per-call fallback to it.
// Mirrors the container contract: container.Buff is the reusable byte
// buffer, container.Slice the float32 destination; the returned slice may
// or may not alias container.Slice. The view argument is only used by the
// objects-bucket fallback — the column does its own copy inside the
// segment lock window.
func (s *Shard) readVectorColumnIntoSliceWithView(ctx context.Context, indexID uint64,
	container *common.VectorSlice, targetVector string, view common.BucketView,
) ([]float32, error) {
	if bucket := s.servableVectorColumnBucket(targetVector); bucket != nil {
		payload, ok := bucket.ColumnarGetVectorPayload(indexID, container.Buff[:0])
		container.Buff = payload
		if ok && len(payload) > 0 {
			return lsmkv.BytesToFloat32s(payload, container.Slice), nil
		}
		// not found in the column (docID written while the flag was
		// disabled, or deleted): fall back to the objects bucket
	}
	return s.readVectorByIndexIDIntoSliceWithView(ctx, indexID, container, targetVector, view)
}

// readMultiVectorColumnIntoSliceWithView is the column-backed twin of
// readMultiVectorByIndexIDIntoSliceWithView. The token matrix is freshly
// allocated (one flat allocation, sub-sliced per token): the multi-vector
// callers return the pooled container before consuming the result, so the
// result must not alias container.Slice — matching the objects-bucket
// path, where MultiVectorFromBinary also allocates per token.
func (s *Shard) readMultiVectorColumnIntoSliceWithView(ctx context.Context, indexID uint64,
	container *common.VectorSlice, targetVector string, view common.BucketView,
) ([][]float32, error) {
	if vecs, ok := s.tryReadMultiVectorColumn(indexID, container, targetVector); ok {
		return vecs, nil
	}
	return s.readMultiVectorByIndexIDIntoSliceWithView(ctx, indexID, container, targetVector, view)
}

// readMultiVectorColumnIntoSlice is the column-backed twin of
// readMultiVectorByIndexIDIntoSlice (the view-less variant used by
// TempMultiVectorForIDThunk).
func (s *Shard) readMultiVectorColumnIntoSlice(ctx context.Context, indexID uint64,
	container *common.VectorSlice, targetVector string,
) ([][]float32, error) {
	if vecs, ok := s.tryReadMultiVectorColumn(indexID, container, targetVector); ok {
		return vecs, nil
	}
	return s.readMultiVectorByIndexIDIntoSlice(ctx, indexID, container, targetVector)
}

// columnMultiVectorByIndexID is the column-backed twin of
// multiVectorByIndexID (MultiVectorForIDThunk — the MUVERA doc-cache load
// path).
func (s *Shard) columnMultiVectorByIndexID(ctx context.Context, indexID uint64,
	targetVector string,
) ([][]float32, error) {
	var container common.VectorSlice
	if vecs, ok := s.tryReadMultiVectorColumn(indexID, &container, targetVector); ok {
		return vecs, nil
	}
	return s.multiVectorByIndexID(ctx, indexID, targetVector)
}

// tryReadMultiVectorColumn reads docID's token matrix from the column.
// Returns (nil, false) when the caller must fall back to the objects
// bucket. The token count is derived from the payload length and the
// column schema's dimensionality.
func (s *Shard) tryReadMultiVectorColumn(indexID uint64, container *common.VectorSlice,
	targetVector string,
) ([][]float32, bool) {
	bucket := s.servableVectorColumnBucket(targetVector)
	if bucket == nil {
		return nil, false
	}
	schema := bucket.ColumnarSchema()
	if schema == nil || !schema.IsVector() {
		return nil, false
	}
	dims := int(schema.Columns[0].Dims)
	if dims == 0 {
		return nil, false
	}

	payload, ok := bucket.ColumnarGetVectorPayload(indexID, container.Buff[:0])
	container.Buff = payload
	if !ok || len(payload) == 0 || len(payload)%(dims*4) != 0 {
		return nil, false
	}

	flat := lsmkv.BytesToFloat32s(payload, nil)
	tokens := len(flat) / dims
	out := make([][]float32, tokens)
	for i := range out {
		out[i] = flat[i*dims : (i+1)*dims : (i+1)*dims]
	}
	return out, true
}

// dropVectorColumn removes the column bucket, its sentinel and the
// in-memory state for the target vector. Called from DropVectorIndex.
func (s *Shard) dropVectorColumn(ctx context.Context, targetVector string) error {
	bucketName := helpers.GetVectorColumnBucketName(targetVector)
	if err := s.removeBucket(ctx, bucketName); err != nil {
		return fmt.Errorf("drop vector column bucket for %q: %w", targetVector, err)
	}
	if err := os.Remove(s.vectorColumnSentinelPath(targetVector)); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove vector column sentinel for %q: %w", targetVector, err)
	}
	s.vectorColumnsMu.Lock()
	delete(s.vectorColumns, targetVector)
	s.vectorColumnsMu.Unlock()
	return nil
}
