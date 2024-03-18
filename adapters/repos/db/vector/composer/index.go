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

package composer

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/flat"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/schema"
	ent "github.com/weaviate/weaviate/entities/vectorindex/composer"
	hnswent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/monitoring"
	bolt "go.etcd.io/bbolt"
)

var composerBucket = []byte("composer")

type VectorIndex interface {
	Dump(labels ...string)
	Add(id uint64, vector []float32) error
	AddBatch(ctx context.Context, id []uint64, vector [][]float32) error
	Delete(id ...uint64) error
	SearchByVector(vector []float32, k int, allow helpers.AllowList) ([]uint64, []float32, error)
	SearchByVectorDistance(vector []float32, dist float32,
		maxLimit int64, allow helpers.AllowList) ([]uint64, []float32, error)
	UpdateUserConfig(updated schema.VectorIndexConfig, callback func()) error
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

type composer struct {
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

func New(cfg Config, uc ent.UserConfig, store *lsmkv.Store) (*composer, error) {
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

	index := &composer{
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
		threshold:                uc.Threeshold,
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
		_, err := tx.CreateBucketIfNotExists(composerBucket)
		return err
	})
	if err != nil {
		return nil, err
	}

	upgraded := false
	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(composerBucket)
		v := b.Get([]byte{0})
		if v == nil {
			return nil
		}

		upgraded = v[0] != 0
		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err, "get composer state")
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

func (composer *composer) Compressed() bool {
	composer.RLock()
	defer composer.RUnlock()
	return composer.index.Compressed()
}

func (composer *composer) AddBatch(ctx context.Context, ids []uint64, vectors [][]float32) error {
	composer.RLock()
	defer composer.RUnlock()
	return composer.index.AddBatch(ctx, ids, vectors)
}

func (composer *composer) Add(id uint64, vector []float32) error {
	composer.RLock()
	defer composer.RUnlock()
	return composer.index.Add(id, vector)
}

func (composer *composer) Delete(ids ...uint64) error {
	composer.RLock()
	defer composer.RUnlock()
	return composer.index.Delete(ids...)
}

func (composer *composer) SearchByVector(vector []float32, k int, allow helpers.AllowList) ([]uint64, []float32, error) {
	composer.RLock()
	defer composer.RUnlock()
	return composer.index.SearchByVector(vector, k, allow)
}

func (composer *composer) SearchByVectorDistance(vector []float32, targetDistance float32, maxLimit int64, allow helpers.AllowList) ([]uint64, []float32, error) {
	composer.RLock()
	defer composer.RUnlock()
	return composer.index.SearchByVectorDistance(vector, targetDistance, maxLimit, allow)
}

func (composer *composer) UpdateUserConfig(updated schema.VectorIndexConfig, callback func()) error {
	parsed, ok := updated.(ent.UserConfig)
	if !ok {
		callback()
		return errors.Errorf("config is not UserConfig, but %T", updated)
	}
	if composer.upgraded.Load() {
		composer.RLock()
		defer composer.RUnlock()
		composer.index.UpdateUserConfig(parsed.HnswUC, callback)
	} else {
		composer.hnswUC = parsed.HnswUC
		composer.RLock()
		defer composer.RUnlock()
		composer.index.UpdateUserConfig(parsed.FlatUC, callback)
	}
	return nil
}

func (composer *composer) Drop(ctx context.Context) error {
	composer.RLock()
	defer composer.RUnlock()
	if err := composer.db.Close(); err != nil {
		return err
	}
	os.Remove(filepath.Join(composer.rootPath, "index.db"))
	return composer.index.Drop(ctx)
}

func (composer *composer) Flush() error {
	composer.RLock()
	defer composer.RUnlock()
	return composer.index.Flush()
}

func (composer *composer) Shutdown(ctx context.Context) error {
	composer.RLock()
	defer composer.RUnlock()
	if err := composer.db.Close(); err != nil {
		return err
	}
	return composer.index.Shutdown(ctx)
}

func (composer *composer) SwitchCommitLogs(ctx context.Context) error {
	composer.RLock()
	defer composer.RUnlock()
	return composer.index.SwitchCommitLogs(ctx)
}

func (composer *composer) ListFiles(ctx context.Context, basePath string) ([]string, error) {
	composer.RLock()
	defer composer.RUnlock()
	return composer.index.ListFiles(ctx, basePath)
}

func (composer *composer) ValidateBeforeInsert(vector []float32) error {
	composer.RLock()
	defer composer.RUnlock()
	return composer.index.ValidateBeforeInsert(vector)
}

func (composer *composer) PostStartup() {
	composer.Lock()
	defer composer.Unlock()
	composer.index.PostStartup()
}

func (composer *composer) Dump(labels ...string) {
	if len(labels) > 0 {
		fmt.Printf("--------------------------------------------------\n")
		fmt.Printf("--  %s\n", strings.Join(labels, ", "))
	}
	fmt.Printf("--------------------------------------------------\n")
	fmt.Printf("ID: %s\n", composer.id)
	fmt.Printf("--------------------------------------------------\n")
}

func (composer *composer) DistanceBetweenVectors(x, y []float32) (float32, bool, error) {
	composer.RLock()
	defer composer.RUnlock()
	return composer.index.DistanceBetweenVectors(x, y)
}

func (composer *composer) ContainsNode(id uint64) bool {
	composer.RLock()
	defer composer.RUnlock()
	return composer.index.ContainsNode(id)
}

func (composer *composer) AlreadyIndexed() uint64 {
	composer.RLock()
	defer composer.RUnlock()
	return composer.index.AlreadyIndexed()
}

func (composer *composer) DistancerProvider() distancer.Provider {
	composer.RLock()
	defer composer.RUnlock()
	return composer.index.DistancerProvider()
}

func (composer *composer) ShouldUpgrade() (bool, int) {
	if !composer.upgraded.Load() {
		return true, int(composer.threshold)
	}
	composer.RLock()
	defer composer.RUnlock()
	return (composer.index).(upgradableIndexer).ShouldUpgrade()
}

func (composer *composer) Upgraded() bool {
	composer.RLock()
	defer composer.RUnlock()
	return composer.upgraded.Load() && composer.index.(upgradableIndexer).Upgraded()
}

func float32SliceFromByteSlice(vector []byte, slice []float32) []float32 {
	for i := range slice {
		slice[i] = math.Float32frombits(binary.LittleEndian.Uint32(vector[i*4:]))
	}
	return slice
}

func (composer *composer) Upgrade(callback func()) error {
	composer.Lock()
	defer composer.Unlock()
	if composer.upgraded.Load() {
		return composer.index.(upgradableIndexer).Upgrade(callback)
	}

	count := composer.index.AlreadyIndexed()
	ids := make([]uint64, 0, count)
	vectors := make([][]float32, 0, count)
	cursor := composer.store.Bucket(helpers.VectorsBucketLSM).Cursor()
	for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
		id := binary.BigEndian.Uint64(k)
		vc := make([]float32, len(v)/4)
		float32SliceFromByteSlice(v, vc)
		ids = append(ids, id)
		vectors = append(vectors, vc)
	}
	cursor.Close()

	index, err := hnsw.New(
		hnsw.Config{
			Logger:                composer.logger,
			RootPath:              composer.rootPath,
			ID:                    composer.id,
			ShardName:             composer.shardName,
			ClassName:             composer.className,
			PrometheusMetrics:     composer.prometheusMetrics,
			VectorForIDThunk:      composer.vectorForIDThunk,
			TempVectorForIDThunk:  composer.tempVectorForIDThunk,
			DistanceProvider:      composer.distanceProvider,
			MakeCommitLoggerThunk: composer.makeCommitLoggerThunk,
		},
		composer.hnswUC,
		composer.tombstoneCallbacks,
		composer.shardCompactionCallbacks,
		composer.shardFlushCallbacks,
		composer.store,
	)
	if err != nil {
		callback()
		return err
	}

	compressionhelpers.Concurrently(uint64(count), func(i uint64) {
		index.Add(ids[i], vectors[i])
	})

	buf := make([]byte, 1)
	buf[0] = 1

	err = composer.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(composerBucket)
		return b.Put([]byte{0}, buf)
	})
	if err != nil {
		return errors.Wrap(err, "update composer")
	}

	composer.index = index
	composer.upgraded.Store(true)
	callback()
	return nil
}
