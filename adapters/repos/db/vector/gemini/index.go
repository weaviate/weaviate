//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package gemini

import (
	"context"

    geminiplugin "example.com/gemini"
	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/schema"
    ent "github.com/weaviate/weaviate/entities/vectorindex/gemini"
)

type gemini struct{

    // plugin object
    plugin *geminiplugin.Gemini
}

/*
func New(cfg Config, uc ent.UserConfig) (*gemini, error) {

    if err := cfg.Validate(); err != nil {
        return nil, errors.Wrap(err, "invalid config")
    }

    if cfg.Logger == nil {
        logger := logrus.New()
        logger.Out = io.Discard
        cfg.Logger = logger
    }

    normalizeOnRead := false
    if cfg.DistanceProvider.Type() == "cosine-dot" {
        normalizeOnRead = true
    }

    vectorCache := newShardedLockCache(cfg.VectorForIDThunk, uc.VectorCacheMaxObjects,
        cfg.Logger, normalizeOnRead, defaultDeletionInterval)

    resetCtx, resetCtxCancel := context.WithCancel(context.Background())
    index := &hnsw{
        maximumConnections: uc.MaxConnections,

        // inspired by original paper and other implementations
        maximumConnectionsLayerZero: 2 * uc.MaxConnections,

        // inspired by c++ implementation
        levelNormalizer:   1 / math.Log(float64(uc.MaxConnections)),
        efConstruction:    uc.EFConstruction,
        flatSearchCutoff:  int64(uc.FlatSearchCutoff),
        nodes:             make([]*vertex, initialSize),
        cache:             vectorCache,
        vectorForID:       vectorCache.get,
        multiVectorForID:  vectorCache.multiGet,
        id:                cfg.ID,
        rootPath:          cfg.RootPath,
        tombstones:        map[uint64]struct{}{},
        logger:            cfg.Logger,
        distancerProvider: cfg.DistanceProvider,
        deleteLock:        &sync.Mutex{},
        tombstoneLock:     &sync.RWMutex{},
        resetLock:         &sync.Mutex{},
        resetCtx:          resetCtx,
        resetCtxCancel:    resetCtxCancel,
        initialInsertOnce: &sync.Once{},
        cleanupInterval:   time.Duration(uc.CleanupIntervalSeconds) * time.Second,

        ef:       int64(uc.EF),
        efMin:    int64(uc.DynamicEFMin),
        efMax:    int64(uc.DynamicEFMax),
        efFactor: int64(uc.DynamicEFFactor),

        metrics: NewMetrics(cfg.PrometheusMetrics, cfg.ClassName, cfg.ShardName),

        randFunc: rand.Float64,
    }

    index.tombstoneCleanupCycle = cyclemanager.New(index.cleanupInterval, index.tombstoneCleanup)
    index.insertMetrics = newInsertMetrics(index.metrics)

    if err := index.init(cfg); err != nil {
        return nil, errors.Wrapf(err, "init index %q", index.id)
    }

    return index, nil
}
*/

func New(cfg Config, uc ent.UserConfig) (*gemini, error) {

    plugin, err := geminiplugin.New()
    if err!= nil {
        return nil, errors.Wrapf( err, "Gemini plugin constructor." )
    }

    idx := &gemini{}
    idx.plugin = plugin
    return idx, nil

}

func (i *gemini) Add(id uint64, vector []float32) error {

    return i.plugin.Add( id, vector )

}

func (i *gemini) Delete(id uint64) error {

    return i.plugin.Delete( id )

}

func (i *gemini) SearchByVector(vector []float32, k int, allow helpers.AllowList) ([]uint64, []float32, error) {

    return i.plugin.SearchByVector( vector, k )

}

func (i *gemini) SearchByVectorDistance(vector []float32, dist float32, maxLimit int64, allow helpers.AllowList) ([]uint64, []float32, error) {

    return i.plugin.SearchByVectorDistance( vector, dist, maxLimit );

}

func (i *gemini) UpdateUserConfig(updated schema.VectorIndexConfig) error {

    return i.plugin.UpdateUserConfig();
    
}


func (i *gemini) Drop(ctx context.Context) error {
    
    return i.plugin.Drop(ctx);

}

func (i *gemini) Flush() error {

    return i.plugin.Flush();

}

func (i *gemini) Shutdown(ctx context.Context) error {

    return i.plugin.Shutdown(ctx);

}

func (i *gemini) PauseMaintenance(ctx context.Context) error {

    return i.plugin.PauseMaintenance(ctx);

}

func (i *gemini) SwitchCommitLogs(ctx context.Context) error {
    
    return i.plugin.SwitchCommitLogs(ctx);
   
}

func (i *gemini) ListFiles(ctx context.Context) ([]string, error) {
    
    return i.plugin.ListFiles(ctx);

}

func (i *gemini) ResumeMaintenance(ctx context.Context) error {
    
    return i.plugin.ResumeMaintenance(ctx);

}

func (i *gemini) ValidateBeforeInsert(vector []float32) error {
    
    return i.plugin.ValidateBeforeInsert(vector);

}

func (i *gemini) PostStartup() {
    
    i.plugin.PostStartup();
   
}

func (i *gemini) Dump(labels ...string) {
    
    i.plugin.Dump();
   
}
