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
	"runtime"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/flat"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	werrors "github.com/weaviate/weaviate/entities/errors"
	schemaconfig "github.com/weaviate/weaviate/entities/schema/config"
	ent "github.com/weaviate/weaviate/entities/vectorindex/dynamic"
	hnswent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/configbase"
	"github.com/weaviate/weaviate/usecases/monitoring"
	bolt "go.etcd.io/bbolt"
)

const composerUpgradedKey = "upgraded"

var dynamicBucket = []byte("dynamic")

type VectorIndex interface {
	Dump(labels ...string)
	Add(id uint64, vector []float32) error
	AddBatch(ctx context.Context, id []uint64, vector [][]float32) error
	Delete(id ...uint64) error
	SearchByVector(vector []float32, k int, allow helpers.AllowList) ([]uint64, []float32, error)
	SearchByVectorDistance(vector []float32, dist float32,
		maxLimit int64, allow helpers.AllowList) ([]uint64, []float32, error)
	UpdateUserConfig(updated schemaconfig.VectorIndexConfig, callback func()) error
	Drop(ctx context.Context) error
	Shutdown(ctx context.Context) error
	Flush() error
	SwitchCommitLogs(ctx context.Context) error
	ListFiles(ctx context.Context, basePath string) ([]string, error)
	PostStartup()
	Compressed() bool
	ValidateBeforeInsert(vector []float32) error
	DistanceBetweenVectors(x, y []float32) (float32, bool, error)
	ContainsNode(id uint64) bool
	DistancerProvider() distancer.Provider
	AlreadyIndexed() uint64
}

type upgradableIndexer interface {
	Upgraded() bool
	Upgrade(callback func()) error
	ShouldUpgrade() (bool, int)
}

type dynamic struct {
	sync.RWMutex
	id                       string
	targetVector             string
	store                    *lsmkv.Store
	logger                   logrus.FieldLogger
	rootPath                 string
	shardName                string
	className                string
	prometheusMetrics        *monitoring.PrometheusMetrics
	vectorForIDThunk         common.VectorForID[float32]
	tempVectorForIDThunk     common.TempVectorForID
	distanceProvider         distancer.Provider
	makeCommitLoggerThunk    hnsw.MakeCommitLogger
	threshold                uint64
	index                    VectorIndex
	upgraded                 atomic.Bool
	tombstoneCallbacks       cyclemanager.CycleCallbackGroup
	shardCompactionCallbacks cyclemanager.CycleCallbackGroup
	shardFlushCallbacks      cyclemanager.CycleCallbackGroup
	hnswUC                   hnswent.UserConfig
	db                       *bolt.DB
}

func New(cfg Config, uc ent.UserConfig, store *lsmkv.Store) (*dynamic, error) {
	if !configbase.Enabled(os.Getenv("ASYNC_INDEXING")) {
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
		TargetVector:     cfg.TargetVector,
		Logger:           cfg.Logger,
		DistanceProvider: cfg.DistanceProvider,
	}

	index := &dynamic{
		id:                       cfg.ID,
		targetVector:             cfg.TargetVector,
		logger:                   logger,
		rootPath:                 cfg.RootPath,
		shardName:                cfg.ShardName,
		className:                cfg.ClassName,
		prometheusMetrics:        cfg.PrometheusMetrics,
		vectorForIDThunk:         cfg.VectorForIDThunk,
		tempVectorForIDThunk:     cfg.TempVectorForIDThunk,
		distanceProvider:         cfg.DistanceProvider,
		makeCommitLoggerThunk:    cfg.MakeCommitLoggerThunk,
		store:                    store,
		threshold:                uc.Threshold,
		tombstoneCallbacks:       cfg.TombstoneCallbacks,
		shardCompactionCallbacks: cfg.ShardCompactionCallbacks,
		shardFlushCallbacks:      cfg.ShardFlushCallbacks,
		hnswUC:                   uc.HnswUC,
	}

	path := filepath.Join(cfg.RootPath, "index.db")

	db, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		return nil, errors.Wrapf(err, "open %q", path)
	}
	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(dynamicBucket)
		return err
	})
	if err != nil {
		return nil, err
	}

	upgraded := false
	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(dynamicBucket)
		v := b.Get([]byte(composerUpgradedKey))
		if v == nil {
			return nil
		}

		upgraded = v[0] != 0
		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err, "get dynamic state")
	}

	index.db = db
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
			},
			index.hnswUC,
			index.tombstoneCallbacks,
			index.shardCompactionCallbacks,
			index.shardFlushCallbacks,
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

func (dynamic *dynamic) Compressed() bool {
	dynamic.RLock()
	defer dynamic.RUnlock()
	return dynamic.index.Compressed()
}

func (dynamic *dynamic) AddBatch(ctx context.Context, ids []uint64, vectors [][]float32) error {
	dynamic.RLock()
	defer dynamic.RUnlock()
	return dynamic.index.AddBatch(ctx, ids, vectors)
}

func (dynamic *dynamic) Add(id uint64, vector []float32) error {
	dynamic.RLock()
	defer dynamic.RUnlock()
	return dynamic.index.Add(id, vector)
}

func (dynamic *dynamic) Delete(ids ...uint64) error {
	dynamic.RLock()
	defer dynamic.RUnlock()
	return dynamic.index.Delete(ids...)
}

func (dynamic *dynamic) SearchByVector(vector []float32, k int, allow helpers.AllowList) ([]uint64, []float32, error) {
	dynamic.RLock()
	defer dynamic.RUnlock()
	return dynamic.index.SearchByVector(vector, k, allow)
}

func (dynamic *dynamic) SearchByVectorDistance(vector []float32, targetDistance float32, maxLimit int64, allow helpers.AllowList) ([]uint64, []float32, error) {
	dynamic.RLock()
	defer dynamic.RUnlock()
	return dynamic.index.SearchByVectorDistance(vector, targetDistance, maxLimit, allow)
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

func (dynamic *dynamic) Drop(ctx context.Context) error {
	dynamic.RLock()
	defer dynamic.RUnlock()
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
	dynamic.RLock()
	defer dynamic.RUnlock()
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

func (dynamic *dynamic) PostStartup() {
	dynamic.Lock()
	defer dynamic.Unlock()
	dynamic.index.PostStartup()
}

func (dynamic *dynamic) Dump(labels ...string) {
	if len(labels) > 0 {
		fmt.Printf("--------------------------------------------------\n")
		fmt.Printf("--  %s\n", strings.Join(labels, ", "))
	}
	fmt.Printf("--------------------------------------------------\n")
	fmt.Printf("ID: %s\n", dynamic.id)
	fmt.Printf("--------------------------------------------------\n")
}

func (dynamic *dynamic) DistanceBetweenVectors(x, y []float32) (float32, bool, error) {
	dynamic.RLock()
	defer dynamic.RUnlock()
	return dynamic.index.DistanceBetweenVectors(x, y)
}

func (dynamic *dynamic) ContainsNode(id uint64) bool {
	dynamic.RLock()
	defer dynamic.RUnlock()
	return dynamic.index.ContainsNode(id)
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
	dynamic.Lock()
	defer dynamic.Unlock()
	if dynamic.upgraded.Load() {
		return dynamic.index.(upgradableIndexer).Upgrade(callback)
	}

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
		},
		dynamic.hnswUC,
		dynamic.tombstoneCallbacks,
		dynamic.shardCompactionCallbacks,
		dynamic.shardFlushCallbacks,
		dynamic.store,
	)
	if err != nil {
		callback()
		return err
	}

	bucket := dynamic.store.Bucket(helpers.VectorsBucketLSM)

	g := werrors.NewErrorGroupWrapper(dynamic.logger)
	workerCount := runtime.GOMAXPROCS(0)
	type task struct {
		id     uint64
		vector []float32
	}

	ch := make(chan task, workerCount)

	for i := 0; i < workerCount; i++ {
		g.Go(func() error {
			for t := range ch {
				err := index.Add(t.id, t.vector)
				if err != nil {
					return err
				}
			}

			return nil
		})
	}

	cursor := bucket.Cursor()

	for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
		id := binary.BigEndian.Uint64(k)
		vc := make([]float32, len(v)/4)
		float32SliceFromByteSlice(v, vc)

		ch <- task{id: id, vector: vc}
	}
	cursor.Close()

	close(ch)

	err = g.Wait()
	if err != nil {
		callback()
		return errors.Wrap(err, "upgrade")
	}

	err = dynamic.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(dynamicBucket)
		return b.Put([]byte(composerUpgradedKey), []byte{1})
	})
	if err != nil {
		return errors.Wrap(err, "update dynamic")
	}

	dynamic.index = index
	dynamic.upgraded.Store(true)
	callback()
	return nil
}
