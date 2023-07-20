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

package db

import (
	"context"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/config"
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
	shutdown          chan struct{}
	startupComplete   atomic.Bool
	resourceScanState *resourceScanState

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

	batchJobQueueCh     chan batchJob
	bm25fJobQueueCh     chan bM25fJob
	shutDownWg          sync.WaitGroup
	maxNumberGoroutines int
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
		logger:              logger,
		config:              config,
		indices:             map[string]*Index{},
		remoteIndex:         remoteIndex,
		nodeResolver:        nodeResolver,
		remoteNode:          sharding.NewRemoteNode(nodeResolver, remoteNodesClient),
		replicaClient:       replicaClient,
		promMetrics:         promMetrics,
		shutdown:            make(chan struct{}),
		batchJobQueueCh:     make(chan batchJob, 100000),
		bm25fJobQueueCh:     make(chan bM25fJob, _NUMCPU/2),
		maxNumberGoroutines: int(math.Round(config.MaxImportGoroutinesFactor * float64(runtime.GOMAXPROCS(0)))),
		resourceScanState:   newResourceScanState(),
	}
	if db.maxNumberGoroutines == 0 {
		return db, errors.New("no workers to add batch-jobs configured.")
	}
	db.shutDownWg.Add(db.maxNumberGoroutines)
	for i := 0; i < db.maxNumberGoroutines; i++ {
		go db.batchWorker()
	}

	for i := 0; i < _NUMCPU/2; i++ {
		go db.bm25Worker()
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
func (d *DB) IndexExists(className schema.ClassName) bool {
	d.indexLock.RLock()
	defer d.indexLock.RUnlock()

	id := indexID(className)
	_, ok := d.indices[id]
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
	return nil
}

func (db *DB) Shutdown(ctx context.Context) error {
	db.shutdown <- struct{}{}

	// shut down the workers that add objects to
	for i := 0; i < db.maxNumberGoroutines; i++ {
		db.batchJobQueueCh <- batchJob{
			index: -1,
		}
	}

	db.indexLock.Lock()
	defer db.indexLock.Unlock()
	for id, index := range db.indices {
		if err := index.Shutdown(ctx); err != nil {
			return errors.Wrapf(err, "shutdown index %q", id)
		}
	}

	db.shutDownWg.Wait() // wait until job queue shutdown is completed

	return nil
}

func (db *DB) batchWorker() {
	for jobToAdd := range db.batchJobQueueCh {
		if jobToAdd.index < 0 {
			db.shutDownWg.Done()
			return
		}
		jobToAdd.batcher.storeSingleObjectInAdditionalStorage(jobToAdd.ctx, jobToAdd.object, jobToAdd.status, jobToAdd.index)
		jobToAdd.batcher.wg.Done()
	}
}

type batchJob struct {
	object  *storobj.Object
	status  objectInsertStatus
	index   int
	ctx     context.Context
	batcher *objectsBatcher
}

func (db *DB) bm25Worker() {
	t := time.NewTicker(time.Millisecond)
	for {
		select {
		case <-db.shutdown:
			return
		case <-t.C:
			job := <-db.bm25fJobQueueCh

			db.logger.WithField("bm25_queue_count", "sub").
				Debugf("items in queue: %d", len(db.bm25fJobQueueCh))
			metric, err := monitoring.GetMetrics().BM25fQueueCount.GetMetricWithLabelValues("class")
			if err == nil {
				metric.Sub(1)
			}

			objs, dists, err := db.search(job.ctx, job.params)
			if err != nil {
				job.response <- bM25fJobResponse{err: err}
			} else {
				job.response <- bM25fJobResponse{
					objects: objs,
					dists:   dists,
				}
			}
		}
	}
}

type bM25fJob struct {
	ctx      context.Context
	params   dto.GetParams
	response chan bM25fJobResponse
}

type bM25fJobResponse struct {
	objects []*storobj.Object
	dists   []float32
	err     error
}
