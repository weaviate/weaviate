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

package db

import (
	"context"
	"math"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/indexcheckpoint"
	"github.com/weaviate/weaviate/entities/replication"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/memwatch"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/replica"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

type DB struct {
	logger            logrus.FieldLogger
	schemaGetter      schemaUC.SchemaGetter
	config            Config
	indices           map[string]*Index
	remoteIndex       sharding.RemoteIndexClient
	replicaClient     replica.Client
	nodeResolver      nodeResolver
	remoteNode        *sharding.RemoteNode
	promMetrics       *monitoring.PrometheusMetrics
	indexCheckpoints  *indexcheckpoint.Checkpoints
	shutdown          chan struct{}
	startupComplete   atomic.Bool
	resourceScanState *resourceScanState
	memMonitor        *memwatch.Monitor

	// indexLock is an RWMutex which allows concurrent access to various indexes,
	// but only one modification at a time. R/W can be a bit confusing here,
	// because it does not refer to write or read requests from a user's
	// perspective, but rather:
	//
	// - Read -> The array containing all indexes is read-only. In other words
	// there will never be a race condition from doing something like index :=
	// indexes[0]. What you do with the Index after retrieving it from the array
	// does not matter. Assuming that it is thread-safe (it is) you can
	// read/write from the index itself. Therefore from a user's perspective
	// something like a parallel import batch and a read-query can happen without
	// any problems.
	//
	// - Write -> The index array is being modified, for example, because a new
	// index is added. This is mutually exclusive with the other case (but
	// hopefully very short).
	//
	//
	// See also: https://github.com/weaviate/weaviate/issues/2351
	//
	// This lock should be used to avoid that the indices-map is changed while iterating over it. To
	// mark a given index in use, lock that index directly.
	indexLock sync.RWMutex

	jobQueueCh              chan job
	asyncIndexRetryInterval time.Duration
	shutDownWg              sync.WaitGroup
	maxNumberGoroutines     int
	batchMonitorLock        sync.Mutex
	ratePerSecond           int
}

func (db *DB) GetSchemaGetter() schemaUC.SchemaGetter {
	return db.schemaGetter
}

func (db *DB) GetSchema() schema.Schema {
	return db.schemaGetter.GetSchemaSkipAuth()
}

func (db *DB) GetConfig() Config {
	return db.config
}

func (db *DB) GetIndices() []*Index {
	out := make([]*Index, 0, len(db.indices))
	for _, index := range db.indices {
		out = append(out, index)
	}

	return out
}

func (db *DB) GetRemoteIndex() sharding.RemoteIndexClient {
	return db.remoteIndex
}

func (db *DB) SetSchemaGetter(sg schemaUC.SchemaGetter) {
	db.schemaGetter = sg
}

func (db *DB) WaitForStartup(ctx context.Context) error {
	err := db.init(ctx)
	if err != nil {
		return err
	}

	db.startupComplete.Store(true)
	db.scanResourceUsage()

	return nil
}

func (db *DB) StartupComplete() bool { return db.startupComplete.Load() }

func New(logger logrus.FieldLogger, config Config,
	remoteIndex sharding.RemoteIndexClient, nodeResolver nodeResolver,
	remoteNodesClient sharding.RemoteNodeClient, replicaClient replica.Client,
	promMetrics *monitoring.PrometheusMetrics,
) (*DB, error) {
	db := &DB{
		logger:                  logger,
		config:                  config,
		indices:                 map[string]*Index{},
		remoteIndex:             remoteIndex,
		nodeResolver:            nodeResolver,
		remoteNode:              sharding.NewRemoteNode(nodeResolver, remoteNodesClient),
		replicaClient:           replicaClient,
		promMetrics:             promMetrics,
		shutdown:                make(chan struct{}),
		asyncIndexRetryInterval: 5 * time.Second,
		maxNumberGoroutines:     int(math.Round(config.MaxImportGoroutinesFactor * float64(runtime.GOMAXPROCS(0)))),
		resourceScanState:       newResourceScanState(),
		memMonitor:              memwatch.NewMonitor(memwatch.LiveHeapReader, debug.SetMemoryLimit, 0.97),
	}

	// make sure memMonitor has an initial state
	db.memMonitor.Refresh()

	if db.maxNumberGoroutines == 0 {
		return db, errors.New("no workers to add batch-jobs configured.")
	}
	if !asyncEnabled() {
		db.jobQueueCh = make(chan job, 100000)
		db.shutDownWg.Add(db.maxNumberGoroutines)
		for i := 0; i < db.maxNumberGoroutines; i++ {
			go db.worker(i == 0)
		}
	} else {
		logger.Info("async indexing enabled")
		w := runtime.GOMAXPROCS(0) - 1
		db.shutDownWg.Add(w)
		db.jobQueueCh = make(chan job, w)
		for i := 0; i < w; i++ {
			go func() {
				defer db.shutDownWg.Done()

				asyncWorker(db.jobQueueCh, db.logger, db.asyncIndexRetryInterval)
			}()
		}
	}

	return db, nil
}

type Config struct {
	RootPath                  string
	QueryLimit                int64
	QueryMaximumResults       int64
	QueryNestedRefLimit       int64
	ResourceUsage             config.ResourceUsage
	MaxImportGoroutinesFactor float64
	MemtablesFlushIdleAfter   int
	MemtablesInitialSizeMB    int
	MemtablesMaxSizeMB        int
	MemtablesMinActiveSeconds int
	MemtablesMaxActiveSeconds int
	TrackVectorDimensions     bool
	ServerVersion             string
	GitHash                   string
	AvoidMMap                 bool
	DisableLazyLoadShards     bool
	Replication               replication.GlobalConfig
}

// GetIndex returns the index if it exists or nil if it doesn't
func (db *DB) GetIndex(className schema.ClassName) *Index {
	db.indexLock.RLock()
	defer db.indexLock.RUnlock()

	id := indexID(className)
	index, ok := db.indices[id]
	if !ok {
		return nil
	}

	return index
}

// IndexExists returns if an index exists
func (db *DB) IndexExists(className schema.ClassName) bool {
	db.indexLock.RLock()
	defer db.indexLock.RUnlock()

	id := indexID(className)
	_, ok := db.indices[id]
	return ok
}

// GetIndexForIncoming returns the index if it exists or nil if it doesn't
func (db *DB) GetIndexForIncoming(className schema.ClassName) sharding.RemoteIndexIncomingRepo {
	db.indexLock.RLock()
	defer db.indexLock.RUnlock()

	id := indexID(className)
	index, ok := db.indices[id]
	if !ok {
		return nil
	}

	return index
}

// DeleteIndex deletes the index
func (db *DB) DeleteIndex(className schema.ClassName) error {
	db.indexLock.Lock()
	defer db.indexLock.Unlock()

	// Get index
	id := indexID(className)
	index := db.indices[id]
	if index == nil {
		return nil
	}

	// Drop index
	index.dropIndex.Lock()
	defer index.dropIndex.Unlock()
	if err := index.drop(); err != nil {
		db.logger.WithField("action", "delete_index").WithField("class", className).Error(err)
	}
	delete(db.indices, id)

	db.promMetrics.DeleteClass(className.String())
	return nil
}

func (db *DB) Shutdown(ctx context.Context) error {
	db.shutdown <- struct{}{}

	if !asyncEnabled() {
		// shut down the workers that add objects to
		for i := 0; i < db.maxNumberGoroutines; i++ {
			db.jobQueueCh <- job{
				index: -1,
			}
		}
	}

	db.indexLock.Lock()
	defer db.indexLock.Unlock()
	for id, index := range db.indices {
		if err := index.Shutdown(ctx); err != nil {
			return errors.Wrapf(err, "shutdown index %q", id)
		}
	}

	if asyncEnabled() {
		// shut down the async workers
		close(db.jobQueueCh)
	}

	db.shutDownWg.Wait() // wait until job queue shutdown is completed

	if asyncEnabled() {
		db.indexCheckpoints.Close()
	}

	return nil
}

func (db *DB) worker(first bool) {
	objectCounter := 0
	checkTime := time.Now().Add(time.Second)
	for jobToAdd := range db.jobQueueCh {
		if jobToAdd.index < 0 {
			db.shutDownWg.Done()
			return
		}
		jobToAdd.batcher.storeSingleObjectInAdditionalStorage(jobToAdd.ctx, jobToAdd.object, jobToAdd.status, jobToAdd.index)
		jobToAdd.batcher.wg.Done()
		objectCounter += 1
		if first && time.Now().After(checkTime) { // only have one worker report the rate per second
			db.batchMonitorLock.Lock()
			db.ratePerSecond = objectCounter * db.maxNumberGoroutines
			db.batchMonitorLock.Unlock()

			objectCounter = 0
			checkTime = time.Now().Add(time.Second)
		}
	}
}

type job struct {
	object  *storobj.Object
	status  objectInsertStatus
	index   int
	ctx     context.Context
	batcher *objectsBatcher

	// async only
	chunk   *chunk
	indexer batchIndexer
	queue   *vectorQueue
}

func asyncWorker(ch chan job, logger logrus.FieldLogger, retryInterval time.Duration) {
	var ids []uint64
	var vectors [][]float32
	var deleted []uint64

	for job := range ch {
		c := job.chunk
		for i := range c.data[:c.cursor] {
			if job.queue.IsDeleted(c.data[i].id) {
				deleted = append(deleted, c.data[i].id)
			} else {
				ids = append(ids, c.data[i].id)
				vectors = append(vectors, c.data[i].vector)
			}
		}

		var err error

		if len(ids) > 0 {
		LOOP:
			for {
				err = job.indexer.AddBatch(job.ctx, ids, vectors)
				if err == nil {
					break LOOP
				}

				if errors.Is(err, context.Canceled) {
					logger.WithError(err).Debugf("skipping indexing batch due to context cancellation")
					break LOOP
				}

				logger.WithError(err).Infof("failed to index vectors, retrying in %s", retryInterval.String())

				t := time.NewTimer(retryInterval)
				select {
				case <-job.ctx.Done():
					// drain the timer
					if !t.Stop() {
						<-t.C
					}
					return
				case <-t.C:
				}
			}
		}

		// only persist checkpoint if we indexed a full batch
		if err == nil {
			job.queue.persistCheckpoint(ids)
		}

		job.queue.releaseChunk(c)

		if len(deleted) > 0 {
			job.queue.ResetDeleted(deleted...)
		}

		ids = ids[:0]
		vectors = vectors[:0]
		deleted = deleted[:0]
	}
}
