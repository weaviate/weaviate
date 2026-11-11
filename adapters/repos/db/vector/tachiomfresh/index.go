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
	"fmt"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/queue"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	ent "github.com/weaviate/weaviate/entities/vectorindex/tachiomfresh"
	"github.com/weaviate/weaviate/usecases/floatcomp"
)

// This file holds what the shard's VectorIndex and VectorIndexMulti
// interfaces (adapters/repos/db/vector_index.go) ask of the index beyond
// insert, delete and search. These are the methods of hfresh.go that are
// not specific to the index, copied where the semantics are the same.

var (
	_ common.VectorIndex      = (*TachiomFresh)(nil)
	_ common.VectorIndexMulti = (*TachiomFresh)(nil)
)

// ErrMultiVectorOnly is returned by the single-vector methods: the index
// holds multi-vectors only. hfresh's multi-vector methods return
// ErrMuveraNotEnabled in the same way when muvera is off.
var ErrMultiVectorOnly = errors.New("tachiomfresh is a multi-vector index: single-vector operations are not supported")

func (t *TachiomFresh) Type() common.IndexType {
	return common.IndexTypeTachiomFresh
}

func (t *TachiomFresh) Multivector() bool {
	return true
}

// Compressed is false: the index stores float32 tokens and no codes of its
// own.
func (t *TachiomFresh) Compressed() bool {
	return false
}

func (t *TachiomFresh) CompressionStats() compressionhelpers.CompressionStats {
	return compressionhelpers.UncompressedStats{}
}

// setSearchBudgets publishes the two search-time budgets under
// the atomics that SearchByMultiVector reads, as hfresh keeps searchProbe
// and rescoreLimit. UpdateUserConfig and the sweeps of the tests set
// them on a running index.
func (t *TachiomFresh) setSearchBudgets(kCentroids, kDocsToScore uint32) {
	atomic.StoreUint32(&t.kCentroids, kCentroids)
	atomic.StoreUint32(&t.kDocsToScore, kDocsToScore)
}

// UpdateUserConfig applies the search-time settings. Every other field is
// refused by ValidateUserConfigUpdate before this is called. copied from
// HFresh.UpdateUserConfig
func (t *TachiomFresh) UpdateUserConfig(updated schemaConfig.VectorIndexConfig, callback func()) error {
	parsed, ok := updated.(ent.UserConfig)
	if !ok {
		callback()
		return errors.Errorf("config is not UserConfig, but %T", updated)
	}

	t.setSearchBudgets(parsed.KCentroids, parsed.KDocsToScore)

	callback()
	return nil
}

// copied from HFresh.Drop
func (t *TachiomFresh) Drop(ctx context.Context, keepFiles bool) error {
	_ = t.Shutdown(ctx)
	// Shard::drop will take care of handling store buckets
	return nil
}

// copied from HFresh.PrepareForBackup, over this index's three queues. Not
// hfresh's: the spread sums of the token types are written first, as Flush
// does. They cannot be recounted from the postings when the backup is
// restored, and the shard flushes the buckets after this method returns.
func (t *TachiomFresh) PrepareForBackup(ctx context.Context) error {
	if err := t.types.persistSpread(); err != nil {
		return fmt.Errorf("persist spread sums: %w", err)
	}

	err := t.Centroids.hnsw.PrepareForBackup(ctx)
	if err != nil {
		return err
	}

	for _, queue := range t.taskQueue.queues() {
		err := queue.PrepareForBackup(ctx)
		if err != nil {
			return fmt.Errorf("pause queue: %w", err)
		}
	}

	return nil
}

// copied from HFresh.ResumeAfterBackup
func (t *TachiomFresh) ResumeAfterBackup(ctx context.Context) error {
	for _, queue := range t.taskQueue.queues() {
		queue.DisableMaintenanceMode()
	}

	return t.Centroids.hnsw.ResumeAfterBackup(ctx)
}

// copied from HFresh.SnapshotMutableFiles
func (t *TachiomFresh) SnapshotMutableFiles(ctx context.Context, basePath, stagingDir string) ([]string, error) {
	return nil, nil
}

// ListFiles lists the commit log of the centroid HNSW and the task queues,
// the files of the index's own directory. The postings and the metadata
// are in LSM buckets, which the shard backs up as it does hfresh's. copied
// from HFresh.ListFiles
func (t *TachiomFresh) ListFiles(ctx context.Context, basePath string) ([]string, error) {
	hnswFiles, err := t.Centroids.hnsw.ListFiles(ctx, basePath)
	if err != nil {
		return nil, err
	}

	queueFiles, err := t.ListQueues(ctx, basePath)
	if err != nil {
		return nil, err
	}

	// combine both slices
	var allFiles []string
	allFiles = append(allFiles, hnswFiles...)
	allFiles = append(allFiles, queueFiles...)

	return allFiles, nil
}

// copied from HFresh.ListQueues
func (t *TachiomFresh) ListQueues(ctx context.Context, basePath string) ([]string, error) {
	var files []string

	for _, queue := range t.taskQueue.queues() {
		f, err := queue.ForceSwitch(ctx, basePath)
		if err != nil {
			return nil, err
		}
		files = append(files, f...)
	}

	return files, nil
}

// queues lists the task queues for the backup methods, in hfresh's order
// without the analyze queue, which this index does not use.
func (tq *TaskQueue) queues() []*queue.DiskQueue {
	return []*queue.DiskQueue{tq.splitQueue, tq.reassignQueue, tq.mergeQueue}
}

// PostStartup is hfresh's without the version map warmup. restore runs the
// warmup before the index is handed out: the doc set is built from it (see
// restore), and ContainsDoc must be right from the first call.
func (t *TachiomFresh) PostStartup(ctx context.Context) {
	t.Centroids.hnsw.PostStartup(ctx)
}

// ContainsDoc reports whether the document is indexed and not deleted,
// from the doc set (docs.go says why not the version map).
func (t *TachiomFresh) ContainsDoc(id uint64) bool {
	return t.docs.contains(id)
}

// Iterate walks the doc set.
func (t *TachiomFresh) Iterate(fn func(id uint64) bool) {
	t.docs.iterate(fn)
}

// copied from HFresh.Preload
func (t *TachiomFresh) Preload(id uint64, vector []float32) {
	// for now, nothing to do here
}

// The single-vector methods: the index holds nothing but multi-vectors.

func (t *TachiomFresh) Add(ctx context.Context, id uint64, vector []float32) error {
	return ErrMultiVectorOnly
}

func (t *TachiomFresh) AddBatch(ctx context.Context, ids []uint64, vectors [][]float32) error {
	return ErrMultiVectorOnly
}

func (t *TachiomFresh) ValidateBeforeInsert(vector []float32) error {
	return ErrMultiVectorOnly
}

func (t *TachiomFresh) SearchByVector(ctx context.Context, vector []float32, k int, allow helpers.AllowList) ([]uint64, []float32, error) {
	return nil, nil, ErrMultiVectorOnly
}

func (t *TachiomFresh) SearchByVectorDistance(ctx context.Context, vector []float32, targetDistance float32, maxLimit int64, allow helpers.AllowList) ([]uint64, []float32, error) {
	return nil, nil, ErrMultiVectorOnly
}

func (t *TachiomFresh) QueryVectorDistancer(queryVector []float32) common.QueryVectorDistancer {
	return common.QueryVectorDistancer{
		DistanceFunc: func(uint64) (float32, error) {
			return 0, ErrMultiVectorOnly
		},
	}
}

// SearchByMultiVectorDistance returns every document within targetDistance
// (the MaxSim distance of SearchByMultiVector). It widens the search until
// a result falls outside that distance or maxLimit is reached. copied from
// HFresh.SearchByMultiVectorDistance, with the loop of hnsw.
func (t *TachiomFresh) SearchByMultiVectorDistance(ctx context.Context, vectors [][]float32, targetDistance float32, maxLimit int64, allow helpers.AllowList) ([]uint64, []float32, error) {
	searchParams := common.NewSearchByDistParams(0, common.DefaultSearchByDistInitialLimit, common.DefaultSearchByDistInitialLimit, maxLimit)
	var resultIDs []uint64
	var resultDist []float32

	recursiveSearch := func() (bool, error) {
		totalLimit := searchParams.TotalLimit()
		ids, dists, err := t.SearchByMultiVector(ctx, vectors, totalLimit, allow)
		if err != nil {
			return false, errors.Wrap(err, "multi-vector search")
		}

		shouldContinue := len(ids) >= totalLimit

		offsetCap := searchParams.OffsetCapacity(ids)
		totalLimitCap := searchParams.TotalLimitCapacity(ids)
		if offsetCap == totalLimitCap {
			return false, nil
		}

		ids, dists = ids[offsetCap:totalLimitCap], dists[offsetCap:totalLimitCap]
		for i := range ids {
			if dists[i] <= targetDistance ||
				floatcomp.InDelta(float64(dists[i]), float64(targetDistance), 1e-6) {
				resultIDs = append(resultIDs, ids[i])
				resultDist = append(resultDist, dists[i])
			} else {
				shouldContinue = false
				break
			}
		}

		return shouldContinue, nil
	}

	// hfresh's loop calls recursiveSearch in its init statement only, so it
	// never widens. This is hnsw's loop (hnsw.searchByVectorDistance).
	shouldContinue, err := recursiveSearch()
	if err != nil {
		return nil, nil, err
	}
	for shouldContinue {
		searchParams.Iterate()
		if searchParams.MaxLimitReached() {
			t.logger.
				WithField("action", "unlimited_multi_vector_search").
				Warnf("maximum search limit of %d results has been reached",
					searchParams.MaximumSearchLimit())
			break
		}
		shouldContinue, err = recursiveSearch()
		if err != nil {
			return nil, nil, err
		}
	}

	return resultIDs, resultDist, nil
}

// QueryMultiVectorDistancer scores one document at a time by the exact
// MaxSim. The document is read into a container owned by the distancer.
// copied from HFresh.QueryMultiVectorDistancer
func (t *TachiomFresh) QueryMultiVectorDistancer(queryVectors [][]float32) common.QueryVectorDistancer {
	var container common.VectorSlice
	distFunc := func(id uint64) (float32, error) {
		docVectors, err := t.readMultiVector(t.ctx, id, &container)
		if err != nil {
			return 0, errors.Wrapf(err, "get multi-vector for id %d", id)
		}
		return t.maxSimScore(queryVectors, docVectors)
	}

	return common.QueryVectorDistancer{DistanceFunc: distFunc}
}
