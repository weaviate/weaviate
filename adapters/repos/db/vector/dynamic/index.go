//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package dynamic

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"go.etcd.io/bbolt"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/flat"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	entcfg "github.com/weaviate/weaviate/entities/config"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	schemaconfig "github.com/weaviate/weaviate/entities/schema/config"
	ent "github.com/weaviate/weaviate/entities/vectorindex/dynamic"
	hnswent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

const composerUpgradedKey = "upgraded"

var dynamicBucket = []byte("dynamic")

type MultiVectorIndex interface {
	AddMulti(ctx context.Context, docId uint64, vector [][]float32) error
	AddMultiBatch(ctx context.Context, docIds []uint64, vectors [][][]float32) error
	DeleteMulti(id ...uint64) error
	SearchByMultiVector(ctx context.Context, vector [][]float32, k int, allow helpers.AllowList) ([]uint64, []float32, error)
	SearchByMultiVectorDistance(ctx context.Context, vector [][]float32, targetDistance float32,
		maxLimit int64, allowList helpers.AllowList) ([]uint64, []float32, error)
	GetKeys(id uint64) (uint64, uint64, error)
	ValidateMultiBeforeInsert(vector [][]float32) error
}

type VectorIndex interface {
	MultiVectorIndex
	Dump(labels ...string)
	Add(ctx context.Context, id uint64, vector []float32) error
	AddBatch(ctx context.Context, id []uint64, vector [][]float32) error
	Delete(id ...uint64) error
	SearchByVector(ctx context.Context, vector []float32, k int, allow helpers.AllowList) ([]uint64, []float32, error)
	SearchByVectorDistance(ctx context.Context, vector []float32, dist float32,
		maxLimit int64, allow helpers.AllowList) ([]uint64, []float32, error)
	UpdateUserConfig(updated schemaconfig.VectorIndexConfig, callback func()) error
	Drop(ctx context.Context) error
	Shutdown(ctx context.Context) error
	Flush() error
	SwitchCommitLogs(ctx context.Context) error
	ListFiles(ctx context.Context, basePath string) ([]string, error)
	PostStartup()
	Compressed() bool
	Multivector() bool
	ValidateBeforeInsert(vector []float32) error
	DistanceBetweenVectors(x, y []float32) (float32, error)
	ContainsDoc(docID uint64) bool
	DistancerProvider() distancer.Provider
	AlreadyIndexed() uint64
	QueryVectorDistancer(queryVector []float32) common.QueryVectorDistancer
	QueryMultiVectorDistancer(queryVector [][]float32) common.QueryVectorDistancer
	// Iterate over all indexed document ids in the index.
	// Consistency or order is not guaranteed, as the index may be concurrently modified.
	// If the callback returns false, the iteration will stop.
	Iterate(fn func(docID uint64) bool)
	Stats() (common.IndexStats, error)
}

type upgradableIndexer interface {
	Upgraded() bool
	Upgrade(callback func()) error
	ShouldUpgrade() (bool, int)
}

type dynamic struct {
	sync.RWMutex
	id                    string
	targetVector          string
	store                 *lsmkv.Store
	logger                logrus.FieldLogger
	rootPath              string
	shardName             string
	className             string
	prometheusMetrics     *monitoring.PrometheusMetrics
	vectorForIDThunk      common.VectorForID[float32]
	tempVectorForIDThunk  common.TempVectorForID[float32]
	distanceProvider      distancer.Provider
	makeCommitLoggerThunk hnsw.MakeCommitLogger
	threshold             uint64
	index                 VectorIndex
	upgraded              atomic.Bool
	upgradeOnce           sync.Once
	tombstoneCallbacks    cyclemanager.CycleCallbackGroup
	hnswUC                hnswent.UserConfig
	db                    *bbolt.DB
	ctx                   context.Context
	cancel                context.CancelFunc
	hnswDisableSnapshots  bool
	hnswSnapshotOnStartup bool
	LazyLoadSegments      bool
}

func New(cfg Config, uc ent.UserConfig, store *lsmkv.Store) (*dynamic, error) {
	if !entcfg.Enabled(os.Getenv("ASYNC_INDEXING")) {
		return nil, errors.New("the dynamic index can only be created under async indexing environment")
	}
	if err := cfg.Validate(); err != nil {
		return nil, errors.Wrap(err, "invalid config")
	}

	logger := cfg.Logger
	if logger == nil {
		l := logrus.New()
		l.Out = io.Discard
		logger = l
	}

	flatConfig := flat.Config{
		ID:               cfg.ID,
		RootPath:         cfg.RootPath,
		TargetVector:     cfg.TargetVector,
		Logger:           cfg.Logger,
		DistanceProvider: cfg.DistanceProvider,
		MinMMapSize:      cfg.MinMMapSize,
		MaxWalReuseSize:  cfg.MaxWalReuseSize,
		LazyLoadSegments: cfg.LazyLoadSegments,
		AllocChecker:     cfg.AllocChecker,
	}

	ctx, cancel := context.WithCancel(context.Background())

	index := &dynamic{
		id:                    cfg.ID,
		targetVector:          cfg.TargetVector,
		logger:                logger,
		rootPath:              cfg.RootPath,
		shardName:             cfg.ShardName,
		className:             cfg.ClassName,
		prometheusMetrics:     cfg.PrometheusMetrics,
		vectorForIDThunk:      cfg.VectorForIDThunk,
		tempVectorForIDThunk:  cfg.TempVectorForIDThunk,
		distanceProvider:      cfg.DistanceProvider,
		makeCommitLoggerThunk: cfg.MakeCommitLoggerThunk,
		store:                 store,
		threshold:             uc.Threshold,
		tombstoneCallbacks:    cfg.TombstoneCallbacks,
		hnswUC:                uc.HnswUC,
		db:                    cfg.SharedDB,
		ctx:                   ctx,
		cancel:                cancel,
		hnswDisableSnapshots:  cfg.HNSWDisableSnapshots,
		hnswSnapshotOnStartup: cfg.HNSWSnapshotOnStartup,
		LazyLoadSegments:      cfg.LazyLoadSegments,
	}

	err := cfg.SharedDB.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(dynamicBucket)
		return err
	})
	if err != nil {
		return nil, errors.Wrap(err, "create dynamic bolt bucket")
	}

	upgraded := false

	err = cfg.SharedDB.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(dynamicBucket)

		v := b.Get(index.dbKey())
		if v == nil {
			return nil
		}

		upgraded = v[0] != 0
		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err, "get dynamic state")
	}

	if upgraded {
		index.upgraded.Store(true)
		hnsw, err := hnsw.New(
			hnsw.Config{
				Logger:                index.logger,
				RootPath:              index.rootPath,
				ID:                    index.id,
				ShardName:             index.shardName,
				ClassName:             index.className,
				PrometheusMetrics:     index.prometheusMetrics,
				VectorForIDThunk:      index.vectorForIDThunk,
				TempVectorForIDThunk:  index.tempVectorForIDThunk,
				DistanceProvider:      index.distanceProvider,
				MakeCommitLoggerThunk: index.makeCommitLoggerThunk,
				DisableSnapshots:      index.hnswDisableSnapshots,
				SnapshotOnStartup:     index.hnswSnapshotOnStartup,
				LazyLoadSegments:      index.LazyLoadSegments,
			},
			index.hnswUC,
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
	}

	return index, nil
}

func (dynamic *dynamic) dbKey() []byte {
	var key []byte
	if dynamic.targetVector == "fef" {
		key = make([]byte, 0, len(composerUpgradedKey)+len(dynamic.targetVector)+1)
		key = append(key, composerUpgradedKey...)
		key = append(key, '_')
		key = append(key, dynamic.targetVector...)
	} else {
		key = []byte(composerUpgradedKey)
	}

	return key
}

func (dynamic *dynamic) getBucketName() string {
	if dynamic.targetVector != "" {
		return fmt.Sprintf("%s_%s", helpers.VectorsBucketLSM, dynamic.targetVector)
	}

	return helpers.VectorsBucketLSM
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

func (dynamic *dynamic) AddMultiBatch(ctx context.Context, ids []uint64, vectors [][][]float32) error {
	dynamic.RLock()
	defer dynamic.RUnlock()
	return dynamic.index.AddMultiBatch(ctx, ids, vectors)
}

func (dynamic *dynamic) Add(ctx context.Context, id uint64, vector []float32) error {
	dynamic.RLock()
	defer dynamic.RUnlock()
	return dynamic.index.Add(ctx, id, vector)
}

func (dynamic *dynamic) AddMulti(ctx context.Context, docId uint64, vectors [][]float32) error {
	dynamic.RLock()
	defer dynamic.RUnlock()
	return dynamic.index.AddMulti(ctx, docId, vectors)
}

func (dynamic *dynamic) Delete(ids ...uint64) error {
	dynamic.RLock()
	defer dynamic.RUnlock()
	return dynamic.index.Delete(ids...)
}

func (dynamic *dynamic) DeleteMulti(ids ...uint64) error {
	dynamic.RLock()
	defer dynamic.RUnlock()
	return dynamic.index.DeleteMulti(ids...)
}

func (dynamic *dynamic) SearchByVector(ctx context.Context, vector []float32, k int, allow helpers.AllowList) ([]uint64, []float32, error) {
	dynamic.RLock()
	defer dynamic.RUnlock()
	return dynamic.index.SearchByVector(ctx, vector, k, allow)
}

func (dynamic *dynamic) SearchByMultiVector(ctx context.Context, vectors [][]float32, k int, allow helpers.AllowList) ([]uint64, []float32, error) {
	dynamic.RLock()
	defer dynamic.RUnlock()
	return dynamic.index.SearchByMultiVector(ctx, vectors, k, allow)
}

func (dynamic *dynamic) SearchByVectorDistance(ctx context.Context, vector []float32, targetDistance float32, maxLimit int64, allow helpers.AllowList) ([]uint64, []float32, error) {
	dynamic.RLock()
	defer dynamic.RUnlock()
	return dynamic.index.SearchByVectorDistance(ctx, vector, targetDistance, maxLimit, allow)
}

func (dynamic *dynamic) SearchByMultiVectorDistance(ctx context.Context, vector [][]float32, targetDistance float32, maxLimit int64, allow helpers.AllowList) ([]uint64, []float32, error) {
	dynamic.RLock()
	defer dynamic.RUnlock()
	return dynamic.index.SearchByMultiVectorDistance(ctx, vector, targetDistance, maxLimit, allow)
}

func (dynamic *dynamic) UpdateUserConfig(updated schemaconfig.VectorIndexConfig, callback func()) error {
	parsed, ok := updated.(ent.UserConfig)
	if !ok {
		callback()
		return errors.Errorf("config is not UserConfig, but %T", updated)
	}
	if dynamic.upgraded.Load() {
		dynamic.RLock()
		defer dynamic.RUnlock()
		dynamic.index.UpdateUserConfig(parsed.HnswUC, callback)
	} else {
		dynamic.hnswUC = parsed.HnswUC
		dynamic.RLock()
		defer dynamic.RUnlock()
		dynamic.index.UpdateUserConfig(parsed.FlatUC, callback)
	}
	return nil
}

func (dynamic *dynamic) GetKeys(id uint64) (uint64, uint64, error) {
	dynamic.RLock()
	defer dynamic.RUnlock()
	return dynamic.index.GetKeys(id)
}

func (dynamic *dynamic) Drop(ctx context.Context) error {
	if dynamic.ctx.Err() != nil {
		// already dropped
		return nil
	}

	// cancel the context before locking to stop any ongoing operations
	// and prevent new ones from starting
	dynamic.cancel()

	dynamic.Lock()
	defer dynamic.Unlock()
	if err := dynamic.db.Close(); err != nil {
		return err
	}
	os.Remove(filepath.Join(dynamic.rootPath, "index.db"))
	return dynamic.index.Drop(ctx)
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

	if err := dynamic.db.Close(); err != nil {
		return err
	}
	return dynamic.index.Shutdown(ctx)
}

func (dynamic *dynamic) SwitchCommitLogs(ctx context.Context) error {
	dynamic.RLock()
	defer dynamic.RUnlock()
	return dynamic.index.SwitchCommitLogs(ctx)
}

func (dynamic *dynamic) ListFiles(ctx context.Context, basePath string) ([]string, error) {
	dynamic.RLock()
	defer dynamic.RUnlock()
	return dynamic.index.ListFiles(ctx, basePath)
}

func (dynamic *dynamic) ValidateBeforeInsert(vector []float32) error {
	dynamic.RLock()
	defer dynamic.RUnlock()
	return dynamic.index.ValidateBeforeInsert(vector)
}

func (dynamic *dynamic) ValidateMultiBeforeInsert(vector [][]float32) error {
	dynamic.RLock()
	defer dynamic.RUnlock()
	return dynamic.index.ValidateMultiBeforeInsert(vector)
}

func (dynamic *dynamic) PostStartup() {
	dynamic.Lock()
	defer dynamic.Unlock()
	dynamic.index.PostStartup()
}

func (dynamic *dynamic) DistanceBetweenVectors(x, y []float32) (float32, error) {
	dynamic.RLock()
	defer dynamic.RUnlock()
	return dynamic.index.DistanceBetweenVectors(x, y)
}

func (dynamic *dynamic) ContainsDoc(docID uint64) bool {
	dynamic.RLock()
	defer dynamic.RUnlock()
	return dynamic.index.ContainsDoc(docID)
}

func (dynamic *dynamic) AlreadyIndexed() uint64 {
	dynamic.RLock()
	defer dynamic.RUnlock()
	return dynamic.index.AlreadyIndexed()
}

func (dynamic *dynamic) DistancerProvider() distancer.Provider {
	dynamic.RLock()
	defer dynamic.RUnlock()
	return dynamic.index.DistancerProvider()
}

func (dynamic *dynamic) QueryVectorDistancer(queryVector []float32) common.QueryVectorDistancer {
	dynamic.RLock()
	defer dynamic.RUnlock()
	return dynamic.index.QueryVectorDistancer(queryVector)
}

func (dynamic *dynamic) QueryMultiVectorDistancer(queryVector [][]float32) common.QueryVectorDistancer {
	dynamic.RLock()
	defer dynamic.RUnlock()
	return dynamic.index.QueryMultiVectorDistancer(queryVector)
}

func (dynamic *dynamic) ShouldUpgrade() (bool, int) {
	if !dynamic.upgraded.Load() {
		return true, int(dynamic.threshold)
	}
	dynamic.RLock()
	defer dynamic.RUnlock()
	return (dynamic.index).(upgradableIndexer).ShouldUpgrade()
}

func (dynamic *dynamic) Upgraded() bool {
	dynamic.RLock()
	defer dynamic.RUnlock()
	return dynamic.upgraded.Load() && dynamic.index.(upgradableIndexer).Upgraded()
}

func float32SliceFromByteSlice(vector []byte, slice []float32) []float32 {
	for i := range slice {
		slice[i] = math.Float32frombits(binary.LittleEndian.Uint32(vector[i*4:]))
	}
	return slice
}

func (dynamic *dynamic) Upgrade(callback func()) error {
	if dynamic.ctx.Err() != nil {
		// already closed
		return dynamic.ctx.Err()
	}

	if dynamic.upgraded.Load() {
		return dynamic.index.(upgradableIndexer).Upgrade(callback)
	}

	dynamic.upgradeOnce.Do(func() {
		enterrors.GoWrapper(func() {
			defer callback()

			err := dynamic.doUpgrade()
			if err != nil {
				dynamic.logger.WithError(err).Error("failed to upgrade index")
				return
			}
		}, dynamic.logger)
	})

	return nil
}

func (dynamic *dynamic) doUpgrade() error {
	// Start with a read lock to prevent reading from the index
	// while it's being dropped or closed.
	// This allows search operations to continue while the index is being
	// upgraded.
	dynamic.RLock()

	index, err := hnsw.New(
		hnsw.Config{
			Logger:                dynamic.logger,
			RootPath:              dynamic.rootPath,
			ID:                    dynamic.id,
			ShardName:             dynamic.shardName,
			ClassName:             dynamic.className,
			PrometheusMetrics:     dynamic.prometheusMetrics,
			VectorForIDThunk:      dynamic.vectorForIDThunk,
			TempVectorForIDThunk:  dynamic.tempVectorForIDThunk,
			DistanceProvider:      dynamic.distanceProvider,
			MakeCommitLoggerThunk: dynamic.makeCommitLoggerThunk,
			DisableSnapshots:      dynamic.hnswDisableSnapshots,
			SnapshotOnStartup:     dynamic.hnswSnapshotOnStartup,
		},
		dynamic.hnswUC,
		dynamic.tombstoneCallbacks,
		dynamic.store,
	)
	if err != nil {
		dynamic.RUnlock()
		return err
	}

	bucket := dynamic.store.Bucket(dynamic.getBucketName())

	cursor := bucket.Cursor()

	for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
		if dynamic.ctx.Err() != nil {
			cursor.Close()
			// context was cancelled, stop processing
			dynamic.RUnlock()
			return dynamic.ctx.Err()
		}

		id := binary.BigEndian.Uint64(k)
		vc := make([]float32, len(v)/4)
		float32SliceFromByteSlice(v, vc)

		err := index.Add(dynamic.ctx, id, vc)
		if err != nil {
			dynamic.logger.WithField("id", id).WithError(err).Error("failed to add vector")
			continue
		}
	}

	cursor.Close()

	// end of read-only zone
	dynamic.RUnlock()

	// Lock the index for writing but check if it was already
	// closed in the meantime
	dynamic.Lock()
	defer dynamic.Unlock()

	if err := dynamic.ctx.Err(); err != nil {
		// already closed
		return errors.Wrap(err, "index was closed while upgrading")
	}

	err = dynamic.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(dynamicBucket)
		return b.Put(dynamic.dbKey(), []byte{1})
	})
	if err != nil {
		return errors.Wrap(err, "update dynamic")
	}

	dynamic.index = index
	dynamic.upgraded.Store(true)

	return nil
}

func (dynamic *dynamic) Iterate(fn func(id uint64) bool) {
	dynamic.index.Iterate(fn)
}

func (dynamic *dynamic) Stats() (common.IndexStats, error) {
	return &DynamicStats{}, errors.New("Stats() is not implemented for dynamic index")
}

type DynamicStats struct{}

func (s *DynamicStats) IndexType() common.IndexType {
	return common.IndexTypeDynamic
}
